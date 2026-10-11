package TestDiscordREST;

use v5.26;
use warnings;
use Exporter 'import';
use Future;
use HTTP::Response;
use IO::Async::Loop;
use JSON::PP;
use Discord::Client::REST;
our @EXPORT_OK = qw(client response error);

sub client {
    my (%args) = @_;
    my $clock = bless { now => 0, timers => [] }, 'TestDiscordREST::Clock';
    my $http = bless { calls => [] }, 'TestDiscordREST::HTTP';
    my $rest = Discord::Client::REST->new(
        token => 'FAKE_TEST_TOKEN',
        loop => IO::Async::Loop->new,
        http => $http,
        clock => sub { $clock->{now} },
        wall_clock => sub { 1000 + $clock->{now} },
        delay => sub { $clock->delay( $_[0] ) },
        %args,
    );
    return ( $rest, $http, $clock );
}

sub response {
    my ( $status, $data, %headers ) = @_;
    return HTTP::Response->new( $status, undef, [%headers], $status == 204 ? '' : JSON::PP->new->utf8->encode($data) );
}

sub error {
    my ($future) = @_;

    # A broken deadline must fail the test instead of waiting on a fake clock forever.
    die 'Expected a failed Future before inspecting its error'
        unless $future->is_failed;
    my ( undef, undef, $error ) = $future->failure;
    return $error;
}

package TestDiscordREST::HTTP;

sub do_request {
    my ( $self, %args ) = @_;
    my $future = Future->new;
    push @{ $self->{calls} }, { %args, future => $future };
    return $future;
}
sub count { scalar @{ $_[0]->{calls} } }
sub call { $_[0]->{calls}[ $_[1] // -1 ] }
sub respond { $_[0]->call( $_[2] )->{future}->done( $_[1] ) }

package TestDiscordREST::Clock;

sub delay {
    my ( $self, $after ) = @_;
    my $future = Future->new;
    push @{ $self->{timers} }, { at => $self->{now} + $after, future => $future };
    return $future;
}

sub timer_count {
    scalar grep { !$_->{future}->is_ready } @{ $_[0]->{timers} };
}

sub advance {
    my ( $self, $seconds ) = @_;
    my $target = $self->{now} + $seconds;
    while (1) {
        my @due = sort { $a->{at} <=> $b->{at} }
            grep { !$_->{future}->is_ready && $_->{at} <= $target } @{ $self->{timers} };
        last unless @due;
        $self->{now} = $due[0]{at};
        $due[0]{future}->done;
    }
    $self->{now} = $target;
    @{ $self->{timers} } = grep { !$_->{future}->is_ready } @{ $self->{timers} };
}

1;
