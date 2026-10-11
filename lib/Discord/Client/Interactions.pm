package Discord::Client::Interactions;

use v5.26;
use strict;
use warnings;
use Carp qw(croak);
use IO::Async::Loop;
use Scalar::Util qw(blessed refaddr);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
use Discord::Client::Error;
use Discord::Client::Interaction;

our $VERSION = '0.001';

sub new {
    my ( $class, %args ) = @_;
    croak 'A command registry is required' unless $args{commands};
    for my $key (qw(max_pending max_seen seen_ttl)) {
        croak "$key must be a positive integer"
            if exists( $args{$key} )
            && ( !defined( $args{$key} ) || ref( $args{$key} ) || $args{$key} !~ /\A[1-9]\d*\z/ );
    }
    for my $key (qw(clock delay on_error)) {
        croak "$key must be a code reference" if exists( $args{$key} ) && ref( $args{$key} ) ne 'CODE';
    }
    return bless {
        loop => $args{loop} // IO::Async::Loop->new,
        commands => $args{commands},
        rest => $args{rest},
        clock => $args{clock} // sub { clock_gettime(CLOCK_MONOTONIC) },
        delay => $args{delay},
        max_pending => $args{max_pending} // 512,
        max_seen => $args{max_seen} // 10000,
        seen_ttl => $args{seen_ttl} // 900,
        on_error => $args{on_error},
        seen => {},
        jobs => {},
        counters => {}
    }, $class;
}

sub _error {
    my ( $self, $message, $category ) = @_;
    return Discord::Client::Error->new( message => $message, category => $category );
}

sub _failed {
    my ( $self, $message, $category ) = @_;
    my $error = $self->_error( $message, $category );
    return $self->{loop}->new_future->fail( $message, $category, $error );
}

sub _report {
    my ( $self, $error, $context ) = @_;
    ++$self->{counters}{errors};
    if ( $self->{on_error} ) {
        my $ok = eval { $self->{on_error}->( $error, $context ); 1 };
        warn "Interaction error handler failed\n" unless $ok;
    }
    else { warn $error->message . "\n" }
}

sub handle {
    my ( $self, $payload, %args ) = @_;
    return $self->_failed( 'Interaction dispatcher is closed', 'shutdown' ) if $self->{closed};
    my $now = $self->{clock}->();

    # A replay window must not expire an ID while its handler can still act.
    for my $id ( keys %{ $self->{seen} } ) {
        delete $self->{seen}{$id}
            if $self->{seen}{$id}{expires} <= $now
            && $self->{seen}{$id}{result}->is_ready;
    }
    my $id = ref($payload) eq 'HASH' ? $payload->{id} : undef;
    if ( defined($id) && !ref($id) && exists $self->{seen}{$id} ) {
        ++$self->{counters}{duplicates};
        return $args{acknowledgement_only} ? $self->{seen}{$id}{ack} : $self->{seen}{$id}{result};
    }
    return $self->_failed( 'Interaction work capacity exceeded', 'queue_full' )
        if keys( %{ $self->{jobs} } ) >= $self->{max_pending} || keys( %{ $self->{seen} } ) >= $self->{max_seen};
    my %context_options = (
        payload => $payload,
        loop => $self->{loop},
        rest => $self->{rest},
        clock => $self->{clock},
    );
    $context_options{delay} = $self->{delay} if $self->{delay};
    for my $name (qw(received_at initial_sender)) {
        $context_options{$name} = $args{$name} if exists $args{$name};
    }
    my $context = eval { Discord::Client::Interaction->new(%context_options) };
    return $self->_failed( 'Invalid interaction payload', 'protocol' ) if !$context || $@;
    if ( $context->acknowledgement->is_failed ) {
        $self->_report( ( $context->acknowledgement->failure )[2], $context );
        return $self->{loop}->new_future->fail( $context->acknowledgement->failure );
    }
    my $result = $self->{loop}->new_future;
    my $job = { ctx => $context, result => $result };
    my $key = refaddr($job);
    $self->{jobs}{$key} = $job;
    $self->{seen}{ $context->id } =
        { expires => $now + $self->{seen_ttl}, ack => $context->acknowledgement, result => $result };
    ++$self->{counters}{received};
    $result->on_ready(
        sub {
            delete $self->{jobs}{$key};
            $context->close unless $context->acknowledgement->is_done;
        }
    );
    $result->on_cancel(
        sub {
            $context->close;
            $job->{handler}->cancel if $job->{handler} && !$job->{handler}->is_ready;
        }
    );
    $job->{handler} = $self->_invoke_handler($job);
    $self->_watch_job($job);
    return $args{acknowledgement_only} ? $context->acknowledgement : $result;
}

sub _invoke_handler {
    my ( $self, $job ) = @_;
    my $context = $job->{ctx};
    my $handler = eval { $self->{commands}->handler_for($context) };
    if ( !$handler || $@ ) {
        my $message = $@ ? 'Invalid interaction command options' : 'No handler for this interaction';
        $self->_record_job_error( $job, $message, 'command' );
        return $self->{loop}->new_future->done;
    }

    my $work = eval { $handler->($context) };
    if ($@) {
        $self->_record_job_error( $job, 'Interaction handler failed', 'handler' );
        return $self->{loop}->new_future->done;
    }
    if ( $context->type == 4 && ref($work) eq 'ARRAY' && !$context->responded ) {
        return $context->autocomplete($work);
    }
    return $work if blessed($work) && $work->isa('Future');
    return $self->{loop}->new_future->done;
}

sub _record_job_error {
    my ( $self, $job, $message, $category ) = @_;
    $job->{error} = $self->_error( $message, $category );
    $self->_report( $job->{error}, $job->{ctx} ) unless $self->{closed};
}

sub _watch_job {
    my ( $self, $job ) = @_;
    my $context = $job->{ctx};
    my $work = $job->{handler};
    my $check = sub { $self->_check_job($job) };
    $work->on_ready(
        sub {
            if ( $context->type == 4 && $work->is_done && !$context->responded ) {
                my ($choices) = $work->result;
                if ( ref($choices) eq 'ARRAY' ) {
                    my $response = $context->autocomplete($choices);
                    $response->on_ready($check);
                }
            }
            $check->();
        }
    );
    $context->acknowledgement->on_ready($check);
    $check->();
}

sub _check_job {
    my ( $self, $job ) = @_;
    my $result = $job->{result};
    return if $result->is_ready;

    my $context = $job->{ctx};
    my $work = $job->{handler};
    my $acknowledgement = $context->acknowledgement;
    if ( $work->is_ready && !$work->is_done && !$job->{error} ) {
        $self->_record_job_error( $job, 'Interaction handler failed', 'handler' );
    }
    return if $result->is_ready;

    if ( $acknowledgement->is_ready && !$acknowledgement->is_done ) {
        my $error = $job->{error};
        unless ($error) {
            if ( $acknowledgement->is_failed ) {
                $error = ( $acknowledgement->failure )[2];
            }
            else {
                $error = $self->_error( 'Interaction acknowledgement cancelled', 'cancelled' );
            }
            $self->_report( $error, $context ) unless $self->{closed};
        }
        $result->fail( $error->message, $error->category, $error ) unless $result->is_ready;
        $work->cancel unless $work->is_ready;
        return;
    }

    # A reply may finish before the handler's deferred work. Both must settle
    # before releasing the bounded active-handler slot.
    return unless $acknowledgement->is_done && $work->is_ready;
    if ( my $error = $job->{error} ) {
        $result->fail( $error->message, $error->category, $error );
    }
    else {
        ++$self->{counters}{handled};
        $result->done;
    }
}

sub stats {
    my ($self) = @_;
    return {
        %{ $self->{counters} },
        pending => scalar( keys %{ $self->{jobs} } ),
        replay_entries => scalar( keys %{ $self->{seen} } ),
    };
}

sub close {
    my ($self) = @_;
    $self->{closed} = 1;
    my @jobs = values %{ $self->{jobs} };
    for my $job (@jobs) {
        $job->{result}->cancel unless $job->{result}->is_ready;
        $job->{ctx}->close;
    }
    $self->{seen} = {};
    return $self->{loop}->new_future->done;
}

1;

__END__

=encoding UTF-8

=head1 NAME

Discord::Client::Interactions - Interaction handler dispatch

=head1 SYNOPSIS

    use Discord::Client::Interactions;

    # Use an existing registry, transport and loop.
    my $dispatcher = Discord::Client::Interactions->new(
        commands => $commands, rest => $rest, loop => $loop,
    );

    # The host drives the loop while this handler runs.
    my $handled = $dispatcher->handle($payload);

=head1 DESCRIPTION

Dispatches command, component, autocomplete and modal handlers from
L<Discord::Client::Commands>. Handlers receive L<Discord::Client::Interaction>.
Acknowledgement and handler completion are tracked separately, so deferred
work can continue while the Gateway receives events.

=head1 METHODS

=head2 new

Returns a dispatcher. C<commands> is required. C<loop> and C<rest> are optional;
otherwise a loop is created and contexts create token-only REST when needed.
C<on_error> is a synchronous callback receiving C<($error, $interaction)>.
Without it, failures warn with sanitized messages.

C<max_pending> defaults to 512 active interactions; C<max_seen> defaults to
10,000 interaction IDs. C<seen_ttl> defaults to 900 seconds; IDs of running
handlers remain protected until completion even after expiry. Capacity
exhaustion fails with C<queue_full>. Tests can inject C<clock> and C<delay>.

=head2 handle

C<handle(payload_hash, received_at =E<gt> $time)> returns a Future completing
when both the initial acknowledgement and handler settle. It resolves with no
values or fails with an SDK Error. Cancellation cancels the context and handler.
C<received_at> is optional monotonic receipt time; otherwise the current clock
is used. Duplicate IDs return the previous Future without running the handler
again.

C<acknowledgement_only =E<gt> 1> returns the initial-response Future instead;
the dispatcher still tracks handler work. An optional C<initial_sender>
callback receives an inline response hashref and returns a Future confirming
acceptance for HTTP delivery.

=head2 stats

Returns a hashref of received, handled, duplicate and error counters, pending
handlers and replay entries.

=head2 close

Cancels pending contexts and handler work, clears replay entries and returns
a completed Future. Supplied REST and Commands objects remain caller-owned.

=head1 LICENSE

Apache License 2.0.

=cut
