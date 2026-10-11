use v5.26;
use warnings;
use Test2::V1;
use File::Temp qw(tempdir);
use Future;
use IO::Async::Loop;
use IO::Async::Listener;
use IO::Async::SSL;
use IO::Socket::SSL::Utils qw(CERT_create PEM_cert2file PEM_key2file CERT_free KEY_free);
use JSON::PP;
use Net::Async::WebSocket::Server;
use Discord::Client::REST;
use lib 't/lib';
use TestDiscordExamples qw(load_example);

my $dir = tempdir( CLEANUP => 1 );
my ( $ca_cert, $ca_key ) = CERT_create(
    subject => { commonName => 'Ping bot test CA' },
    purpose => 'sslCA',
);
my ( $cert, $key ) = CERT_create(
    issuer => [ $ca_cert, $ca_key ],
    purpose => 'server',
    subject => { commonName => '127.0.0.1' },
    subjectAltNames => [ [ 'IP', '127.0.0.1' ] ],
    not_before => time() - 60,
    not_after => time() + 3600
);
PEM_cert2file( $ca_cert, "$dir/ca.pem" );
PEM_cert2file( $cert, "$dir/cert.pem" );
PEM_key2file( $key, "$dir/key.pem" );
CERT_free($cert);
KEY_free($key);
CERT_free($ca_cert);
KEY_free($ca_key);
my $loop = IO::Async::Loop->new;
my ( @clients, @streams, @requests, @errors );
my ( $gateway_url, $ws );
my $interaction = sub {
    my ( $id, $type, $data ) = @_;
    return {
        op => 0,
        s => $id,
        t => 'INTERACTION_CREATE',
        d => {
            id => "$id",
            application_id => '456',
            token => 'FAKE_INTERACTION_TOKEN',
            type => $type,
            data => $data
        }
    };
};
my $server = Net::Async::WebSocket::Server->new(
    on_client => sub {
        my ( undef, $client ) = @_;
        push @clients, $client;
        $ws = $client;
        $client->configure(
            on_text_frame => sub {
                my $packet = JSON::PP->new->decode( $_[1] );
                if ( $packet->{op} == 2 ) {
                    $client->send_text_frame(
                        JSON::PP->new->encode(
                            {
                                op => 0,
                                s => 1,
                                t => 'READY',
                                d => {
                                    session_id => 'ping-wire',
                                    resume_gateway_url => $gateway_url,
                                    application => { id => '456' },
                                    user => { id => '123' }
                                }
                            }
                        )
                    );
                    $client->send_text_frame(
                        JSON::PP->new->encode( $interaction->( 100, 2, { name => 'button', type => 1 } ) ) );
                }
                elsif ( $packet->{op} == 1 ) {
                    $client->send_text_frame('{"op":11}');
                }
            },
            on_close_frame => sub { $client->close }
        );
        $client->send_text_frame('{"op":10,"d":{"heartbeat_interval":1000}}');
    }
);
$loop->add($server);
$server->listen(
    addr => { family => 'inet', socktype => 'stream', ip => '127.0.0.1', port => 0 },
    extensions => ['SSL'],
    SSL_cert_file => "$dir/cert.pem",
    SSL_key_file => "$dir/key.pem"
)->get;
$gateway_url = 'wss://127.0.0.1:' . $server->read_handle->sockport . '/';

my $http_server = IO::Async::Listener->new(
    on_stream => sub {
        my ( undef, $stream ) = @_;
        push @streams, $stream;
        $stream->configure(
            on_read => sub {
                my ( $stream, $buffer ) = @_;
                my $end = index( $$buffer, "\r\n\r\n" );
                return 0 if $end < 0;
                $end += 4;
                my ($length) = substr( $$buffer, 0, $end ) =~ /^Content-Length: (\d+)/mi;
                $length //= 0;
                return 0 if length($$buffer) < $end + $length;
                my $wire = substr( $$buffer, 0, $end + $length, '' );
                push @requests, $wire;

                if ( $wire =~ m{^GET /api/v10/gateway/bot } ) {
                    my $body = JSON::PP->new->encode(
                        {
                            url => $gateway_url,
                            shards => 1,
                            session_start_limit => {
                                total => 10,
                                remaining => 10,
                                reset_after => 100000,
                                max_concurrency => 1
                            }
                        }
                    );
                    $stream->write( "HTTP/1.1 200 OK\r\nContent-Length: "
                            . length($body)
                            . "\r\nConnection: close\r\n\r\n$body" );
                }
                else {
                    T2->like(
                        $wire,
                        qr{^POST /api/v10/interactions/(?:100|101)/FAKE_INTERACTION_TOKEN/callback },
                        'actual interaction callback route'
                    );
                    T2->unlike( $wire, qr/^Authorization:/mi, 'callback excludes Bot authorization' );
                    my $body = JSON::PP->new->utf8->decode( substr( $wire, $end ) );
                    if ( $wire =~ m{/100/} ) {
                        T2->is( $body->{data}{content}, 'Press the button.', 'example slash reply' );
                        T2->is( $body->{data}{components}[0]{components}[0]{custom_id},
                            'hello', 'example button payload' );
                        $ws->send_text_frame(
                            JSON::PP->new->encode(
                                $interaction->( 101, 3, { custom_id => 'hello', component_type => 2 } )
                            )
                        );
                    }
                    else {
                        T2->is( $body->{data}{content}, 'Hello!', 'example button reply' );
                        T2->is( $body->{data}{flags}, 64, 'button reply ephemeral' );
                    }
                    $stream->write("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
                }
                $stream->close_when_empty;
                return 0;
            }
        );
        $loop->add($stream);
    }
);
$loop->add($http_server);
$http_server->listen( addr => { family => 'inet', socktype => 'stream', ip => '127.0.0.1', port => 0 } )->get;
my $done = $loop->new_future;
my $acks = 0;
my $rest = Discord::Client::REST->new(
    token => 'FAKE_WIRE_TOKEN',
    loop => $loop,
    base_url => 'http://127.0.0.1:' . $http_server->read_handle->sockport . '/api/v10',
    on_diagnostic => sub {
        $done->done
            if $_[0]{type} eq 'http_response' && $_[0]{status} == 204 && ++$acks == 2;
    }
);
my $bot = load_example(
    'examples/components.pl',
    token => 'FAKE_WIRE_TOKEN',
    rest => $rest,
    loop => $loop,
    gateway_options => { websocket_options => { tls => { SSL_ca_file => "$dir/ca.pem" } } },
    on_error => sub { push @errors, $_[0] }
);
my $started = $bot->start;
$loop->await( Future->wait_any( $done, $loop->timeout_future( after => 5 ) ) );
T2->ok( $started->is_done && $done->is_done,
    'documented example handles slash and button over actual REST/TLS Gateway' );
T2->is( $bot->interactions->stats->{handled}, 2, 'both handlers and acknowledgements complete' );
T2->is( scalar(@requests), 3, 'only discovery and two callbacks; no startup sync' );
T2->is( scalar(@errors), 0, 'no integration errors' );
$bot->close->get;
$rest->close->get;
$_->close for @clients, @streams;
$server->close;
$http_server->close;
T2->done_testing;
