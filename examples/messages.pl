#!/usr/bin/env perl
use v5.26;
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Discord::Client;

# Enable Message Content in the Developer Portal before running this example.
my $bot = Discord::Client->new(
    token => $ENV{DISCORD_BOT_TOKEN},
    intents => [ 'GUILDS', 'GUILD_MESSAGES', 'DIRECT_MESSAGES', 'MESSAGE_CONTENT' ],
);

# A message already knows how to reply and which channel it belongs to.
sub respond_to_message {
    my ( $message, $bot ) = @_;
    return if $message->is_from_bot;
    return unless $message->content eq '!ping';

    return $message->reply('Pong!');
}
$bot->on_message( \&respond_to_message );

# Run the bot; message events need no application-command deployment.
$bot->run unless caller;
$bot;
