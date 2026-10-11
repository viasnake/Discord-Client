package Discord::Client::Gateway::WebSocket;

use v5.26;
use strict;
use warnings;
use Carp qw(croak);
use Net::Async::WebSocket::Client;
use Future;
use Protocol::WebSocket::Frame;
use Scalar::Util qw(weaken);
use URI;

our $VERSION = '0.001';

sub new {
    my ( $class, %args ) = @_;
    my $self = bless { %args, closed => 0 }, $class;
    my $weak = $self;
    weaken($weak);
    my $client = Discord::Client::Gateway::WebSocket::Client->new(
        on_parser_error => sub { $weak->_notify(4002) if $weak },
        on_text_frame => sub { $weak->{on_text}->( $_[1] ) if $weak && !$weak->{closed} },
        on_binary_frame => sub { $weak->_notify(4002) if $weak },
        on_close_frame => sub {
            my $bytes = $_[1];
            $weak->_notify( length($bytes) >= 2 ? unpack( 'n', $bytes ) : 1005 ) if $weak;
        },
        on_ping_frame => sub { $_[0]->send_pong_frame( $_[1] ) if $weak && !$weak->{closed} },
        on_closed => sub { $weak->_notify(1006) if $weak },
        on_read_error => sub { $weak->_notify(1006) if $weak },
        on_write_error => sub { $weak->_notify(1006) if $weak },
    );
    $self->{client} = $client;
    $args{loop}->add($client);
    return $self;
}

sub connect {
    my ( $self, $url ) = @_;
    my $uri = URI->new($url);
    croak 'Gateway URL must use WSS'
        unless ( $uri->scheme // '' ) eq 'wss'
        || ( $self->{allow_insecure} && ( $uri->scheme // '' ) eq 'ws' );
    return $self->{client}->connect(
        url => $uri,
        %{ $self->{tls} // {} },
        SSL_verify_mode => 1,
        SSL_verifycn_scheme => 'http'
    );
}

sub send { $_[0]->{client}->send_text_frame( $_[1] ) }

sub _notify {
    my ( $self, $code ) = @_;
    return if $self->{closed}++;
    $self->{on_close}->($code);
    $self->_dispose;
}

sub _dispose {
    my ($self) = @_;
    $self->{client}->configure(
        map { $_ => undef }
            qw(on_text_frame on_binary_frame on_close_frame
            on_ping_frame on_closed on_read_error on_write_error on_read on_parser_error)
    );
    $self->{client}->close;
}

sub abort {
    my ($self) = @_;
    $self->{closed} = 1;
    $self->_dispose;
    return $self->{loop}->new_future->done;
}

sub close {
    my ( $self, $code ) = @_;
    return $self->abort unless $self->{client}->write_handle;
    $self->{closed} = 1;
    my $write = eval { $self->{client}->send_close_frame( pack( 'n', $code // 1000 ) ) };
    return $self->abort unless $write;
    my $timeout = $self->{loop}->delay_future( after => 1 );
    my $wait = Future->wait_any( $write, $timeout );
    my $result = $self->{loop}->new_future;
    $wait->on_ready(
        sub {
            $self->_dispose;
            $result->done unless $result->is_ready;
        }
    );
    $result->on_cancel(
        sub {
            $wait->cancel;
            $self->_dispose;
        }
    );
    return $result;
}

package Discord::Client::Gateway::WebSocket::Client;

use parent 'Net::Async::WebSocket::Client';

sub _init {
    my ($self) = @_;
    $self->SUPER::_init;
    $self->{framebuffer} = Protocol::WebSocket::Frame->new( max_payload_size => 16 * 1024 * 1024 );
}

sub configure {
    my ( $self, %args ) = @_;
    $self->{on_parser_error} = delete $args{on_parser_error} if exists $args{on_parser_error};
    $self->SUPER::configure(%args);
}

sub on_read {
    my ( $self, @args ) = @_;
    my $result = eval { $self->SUPER::on_read(@args) };
    if ($@) {
        $self->{on_parser_error}->() if $self->{on_parser_error};
        return 0;
    }
    return $result;
}

sub _do_handshake {
    my ( $self, @args ) = @_;
    my $future = $self->SUPER::_do_handshake(@args);
    my $reader = $self->can_event('on_read');
    $self->SUPER::configure(
        on_read => sub {
            my @read_args = @_;
            my $result = eval { $reader->(@read_args) };
            if ($@) {
                $future->fail( 'Invalid WebSocket handshake', 'protocol' ) unless $future->is_ready;
                $self->{on_parser_error}->() if $self->{on_parser_error};
                return 0;
            }

            # The HTTP upgrade and first Gateway frame can share one socket read.
            # Re-enter the Stream reader after switching away from the handshake.
            return $future->is_done ? 1 : $result;
        }
    );
    return $future;
}

1;
