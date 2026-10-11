#!/usr/bin/env perl
use v5.26;
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use Discord::Client;

# Configure a bot with a small asynchronous workflow.
my $bot = Discord::Client->new(
    token => $ENV{DISCORD_BOT_TOKEN},
    intents => ['GUILDS'],
);

# Acknowledge first, yield to the loop, then complete the private reply.
sub prepare_report {
    my ( $interaction, $bot ) = @_;

    return $interaction->defer( ephemeral => 1 )->then(
        sub {
            return $bot->loop->delay_future( after => 1 );
        }
    )->then(
        sub {
            return $interaction->reply('Your report is ready.');
        }
    );
}

# Register the workflow while leaving deployment explicit.
$bot->command(
    report => (
        description => 'Prepare a report asynchronously',
        handler => sub {
            my ($interaction) = @_;
            return prepare_report( $interaction, $bot );
        },
    ),
);

# Run the bot, or expose its declarations to the deployment script.
$bot->run unless caller;
$bot;
