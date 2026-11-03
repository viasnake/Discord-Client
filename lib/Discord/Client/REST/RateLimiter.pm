package Discord::Client::REST::RateLimiter;

use v5.26;
use strict;
use warnings;
use JSON::PP ();

our $VERSION = '0.001';

# Internal scheduler. One request per bucket avoids speculative quota use and
# out-of-order updates; unrelated buckets can still make progress concurrently.
sub new {
    my ( $class, %args ) = @_;
    return bless {
        %args,
        jobs => [],
        routes => {},
        buckets => {},
        active => 0,
        global_until => {},
        stopped => undef,
        counters => {},
    }, $class;
}

sub submit {
    my ( $self, %args ) = @_;
    my $future = $self->{loop}->new_future;
    return $future->fail( 'REST client is closed', 'shutdown' ) if $self->{stopped};
    return $future->fail( 'Bot authentication is invalid', 'authentication' )
        if $self->{auth_failed} && $args{auth};
    return $future->fail( 'REST request capacity exceeded', 'queue_full' )
        if @{ $self->{jobs} } >= $self->{max_pending};
    $self->_prune;
    if ( !exists $self->{routes}{ $args{route} } ) {
        return $future->fail( 'REST bucket capacity exceeded', 'queue_full' )
            if keys( %{ $self->{routes} } ) >= $self->{max_buckets};
        $self->{routes}{ $args{route} } = { active => 0, blocked => 0 };
    }
    my $job = { %args, future => $future, retries => 0 };
    push @{ $self->{jobs} }, $job;
    $self->_emit('request_queued');
    $future->on_cancel(
        sub {
            $self->_remove($job);
            $self->_emit('request_cancelled');
            my $io = delete $job->{io};
            $io->cancel if $io && !$io->is_ready;
            $self->_pump;
        }
    );
    $self->_pump;
    return $future;
}

sub _root {
    my ($bucket) = @_;
    my @parents;
    while ( $bucket->{parent} ) {
        push @parents, $bucket;
        $bucket = $bucket->{parent};
    }
    $_->{parent} = $bucket for @parents;
    return $bucket;
}

sub _prune {
    my ($self) = @_;
    return if keys( %{ $self->{routes} } ) < $self->{max_buckets};
    my %used = map { $_->{route} => 1 } @{ $self->{jobs} };
    my $now = $self->{clock}->();
    for my $route ( keys %{ $self->{routes} } ) {
        my $bucket = _root( $self->{routes}{$route} );
        next
            if $used{$route}
            || $bucket->{active}
            || $bucket->{blocked} > $now
            || ( $bucket->{reset} // 0 ) > $now;
        delete $self->{routes}{$route};
    }
    my %live = map { _root($_) => 1 } values %{ $self->{routes} };
    for my $key ( keys %{ $self->{buckets} } ) {
        delete $self->{buckets}{$key} unless $live{ _root( $self->{buckets}{$key} ) };
    }
}

sub _pump {
    my ($self) = @_;
    return if $self->{pumping} || $self->{stopped};
    local $self->{pumping} = 1;
    if ( my $timer = delete $self->{timer} ) {
        $timer->cancel unless $timer->is_ready;
    }
    my $earliest;

    # Restart after each send: mock transports may finish synchronously and
    # callers may enqueue or cancel from completion callbacks.
    while ( $self->{active} < $self->{max_in_flight} ) {
        my $now = $self->{clock}->();
        my $candidate;
        $earliest = undef;
        for my $job ( @{ $self->{jobs} } ) {
            next if $job->{active};
            my $bucket = _root( $self->{routes}{ $job->{route} } );
            next if $bucket->{active};
            if ( defined( $bucket->{reset} ) && $bucket->{reset} <= $now ) {
                delete @{$bucket}{qw(reset remaining)};
            }
            my $at = $bucket->{blocked};
            my $global_at = $self->{global_until}{ $job->{global_scope} } // 0;
            $at = $global_at if $global_at > $at;
            if ( defined( $bucket->{remaining} ) && $bucket->{remaining} <= 0 ) {

                # With no reset time, allow one probe after the previous
                # response rather than permanently wedging this route.
                $at = $bucket->{reset} if defined( $bucket->{reset} ) && $bucket->{reset} > $at;
            }
            if ( $at > $now ) {
                $earliest = $at if !defined($earliest) || $at < $earliest;
                next;
            }
            $candidate = $job;
            last;
        }
        last unless $candidate;
        $self->_send($candidate);
    }
    if ( defined $earliest && !$self->{stopped} ) {
        my $delay = $earliest - $self->{clock}->();
        $delay = 0 if $delay < 0;
        my $timer = $self->{delay}->($delay);
        $self->{timer} = $timer;
        $timer->on_done(
            sub {
                delete $self->{timer};
                $self->_pump;
            }
        );
    }
}

sub _send {
    my ( $self, $job ) = @_;
    my $bucket = _root( $self->{routes}{ $job->{route} } );
    $job->{bucket} = $bucket;
    $job->{active} = 1;
    ++$bucket->{active};
    ++$self->{active};
    $self->_emit('http_started');
    return if $job->{removed} || $self->{stopped};
    --$bucket->{remaining} if defined( $bucket->{remaining} ) && $bucket->{remaining} > 0;
    my $io = eval { $job->{send}->() };

    if ( !$io || !eval { $io->isa('Future') } ) {
        $self->_finish( $job, 'HTTP transport failed', 'transport' );
        return;
    }
    $job->{io} = $io;
    $io->on_ready(
        sub {
            delete $job->{io};
            return if $job->{removed};
            $self->_release($job);
            if ( $io->is_cancelled ) {
                $self->_finish( $job, 'HTTP transport was cancelled', 'transport' );
            }
            elsif ( $io->is_failed ) {
                my ( undef, $category, $response ) = $io->failure;

                # Some injected HTTP clients fail on non-2xx responses. Preserve
                # those responses so 429 and authentication handling still apply.
                if ( ref($response) && eval { $response->isa('HTTP::Response') } ) {
                    $self->_response( $job, $response );
                }
                else {
                    $category = ( $category // '' ) =~ /\A(?:timeout|stall_timeout)\z/ ? 'timeout' : 'transport';
                    $self->_finish( $job, 'HTTP request failed', $category );
                }
            }
            else {
                my ($response) = $io->result;
                if ( !ref($response) || !eval { $response->isa('HTTP::Response') } ) {
                    $self->_finish( $job, 'Invalid HTTP transport response', 'transport' );
                }
                else {
                    $self->_response( $job, $response );
                }
            }
            $self->_pump;
        }
    );
}

sub _number {
    my ($value) = @_;
    return undef
        unless defined($value)
        && !ref($value)
        && $value =~ /\A\d+(?:\.\d+)?\z/;
    return 0 + $value;
}

sub _response {
    my ( $self, $job, $response ) = @_;
    my $now = $self->{clock}->();
    $self->_emit( 'http_response', status => $response->code );
    return if $job->{removed} || $self->{stopped};

    my $bucket = $self->_response_bucket( $job, $response );
    $self->_update_bucket_limits( $bucket, $response, $now );
    if ( $response->code == 401 && $job->{auth} ) {
        $self->_invalidate_authentication( $job, $response );
        return;
    }
    if ( $response->code == 429 ) {
        $self->_handle_rate_limit( $job, $bucket, $response, $now );
        return;
    }
    $self->_finish( $job, undef, undef, $response );
}

sub _response_bucket {
    my ( $self, $job, $response ) = @_;
    my $bucket = _root( $self->{routes}{ $job->{route} } );
    my $hash = $response->header('X-RateLimit-Bucket');
    if ( defined($hash) && length($hash) ) {
        my $key = $hash . ':' . $job->{major};
        my $known = $self->{buckets}{$key};
        if ( $known && _root($known) != $bucket ) {
            $known = _root($known);
            $known->{active} += $bucket->{active};
            for my $field (qw(blocked reset)) {
                $known->{$field} = $bucket->{$field}
                    if defined( $bucket->{$field} )
                    && ( !defined( $known->{$field} ) || $bucket->{$field} > $known->{$field} );
            }
            if ( defined $bucket->{remaining} ) {
                $known->{remaining} = $bucket->{remaining}
                    if !defined( $known->{remaining} ) || $bucket->{remaining} < $known->{remaining};
            }
            $bucket->{parent} = $known;
            $bucket = $known;
            $_ = _root($_) for values %{ $self->{routes} }, values %{ $self->{buckets} };
        }
        $self->{buckets}{$key} = $bucket
            if exists( $self->{buckets}{$key} )
            || keys( %{ $self->{buckets} } ) < $self->{max_buckets};
    }
    return $bucket;
}

sub _update_bucket_limits {
    my ( $self, $bucket, $response, $now ) = @_;
    my $remaining = _number( $response->header('X-RateLimit-Remaining') );
    if ( defined $remaining ) {
        $bucket->{remaining} = $remaining
            if !$bucket->{active}
            || !defined( $bucket->{remaining} )
            || $remaining < $bucket->{remaining};
    }
    my $after = _number( $response->header('X-RateLimit-Reset-After') );
    if ( !defined $after ) {
        my $epoch = _number( $response->header('X-RateLimit-Reset') );
        $after = $epoch - $self->{wall_clock}->() if defined $epoch;
    }
    if ( defined $after ) {
        $after = 0 if $after < 0;
        my $reset = $now + $after;
        $bucket->{reset} = $reset if !defined( $bucket->{reset} ) || $reset > $bucket->{reset};
    }
}

sub _invalidate_authentication {
    my ( $self, $job, $response ) = @_;
    $self->{auth_failed} = 1;
    $self->_finish( $job, undef, undef, $response );
    my @queued = @{ $self->{jobs} };
    for my $queued (@queued) {
        next if $queued->{active} || !$queued->{auth};
        $self->_finish( $queued, 'Bot authentication is invalid', 'authentication' );
    }
}

sub _handle_rate_limit {
    my ( $self, $job, $bucket, $response, $now ) = @_;
    if ( $self->{auth_failed} && $job->{auth} ) {
        $self->_finish( $job, 'Bot authentication is invalid', 'authentication', $response );
        return;
    }
    my $data = eval { JSON::PP->new->utf8->decode( $response->content ) };
    $data = {} unless ref($data) eq 'HASH';
    my $retry = _number( $data->{retry_after} );
    my $header = _number( $response->header('Retry-After') );
    $retry = $header if defined($header) && ( !defined($retry) || $header > $retry );
    if ( !defined($retry) || $retry <= 0 ) {
        $self->_finish( $job, 'Rate limit response has no positive retry delay', 'rate_limit', $response );
        return;
    }
    my $until = $now + $retry;
    my $global =
           $data->{global}
        || ( $response->header('X-RateLimit-Global') // '' ) eq 'true'
        || ( $response->header('X-RateLimit-Scope') // '' ) eq 'global';
    $self->_emit( 'rate_limit', global => $global ? 1 : 0, retry_after => $retry );
    return if $job->{removed} || $self->{stopped};
    if ($global) {
        my $scope = $job->{global_scope};
        $self->{global_until}{$scope} = $until if $until > ( $self->{global_until}{$scope} // 0 );
    }
    else {
        $bucket->{blocked} = $until if $until > $bucket->{blocked};
    }
    if ( $job->{retries}++ >= $self->{max_retries} ) {
        $self->_finish( $job, 'Rate limit retry budget exhausted', 'rate_limit', $response );
    }
    else {
        $self->_emit('request_retry');
    }

    # Otherwise the same job remains at its original queue position.
}

sub _release {
    my ( $self, $job ) = @_;
    return unless delete $job->{active};
    --_root( $job->{bucket} )->{active};
    --$self->{active};
    delete $job->{bucket};
}

sub _remove {
    my ( $self, $job ) = @_;
    return if $job->{removed}++;
    $self->_release($job);
    @{ $self->{jobs} } = grep { $_ != $job } @{ $self->{jobs} };
}

sub _finish {
    my ( $self, $job, $message, $category, $response ) = @_;
    $self->_remove($job);
    return if $job->{future}->is_ready;
    $self->_emit( 'request_finished', category => $category // 'response' );
    if ( defined $category ) {
        $job->{future}->fail( $message, $category, $response );
    }
    else {
        $job->{future}->done($response);
    }
}

sub _emit {
    my ( $self, $type, %fields ) = @_;
    ++$self->{counters}{$type};
    return unless $self->{on_diagnostic};
    my $ok = eval {
        $self->{on_diagnostic}->(
            {
                type => $type,
                %fields,
                pending => scalar( @{ $self->{jobs} } ),
                active => $self->{active}
            }
        );
        1;
    };
    warn "REST diagnostic handler failed\n" unless $ok;
}

sub stats {
    my ($self) = @_;
    return { %{ $self->{counters} }, pending => scalar( @{ $self->{jobs} } ), active => $self->{active} };
}

sub close {
    my ($self) = @_;
    $self->{stopped} = 1;
    if ( my $timer = delete $self->{timer} ) {
        $timer->cancel unless $timer->is_ready;
    }
    my @jobs = @{ $self->{jobs} };
    for my $job (@jobs) {
        my $io = delete $job->{io};
        $self->_finish( $job, 'REST client is closed', 'shutdown' );
        $io->cancel if $io && !$io->is_ready;
    }
    $self->{routes} = {};
    $self->{buckets} = {};
    return $self->{loop}->new_future->done;
}

1;
