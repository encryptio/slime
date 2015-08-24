#!/usr/bin/perl
use warnings;
use strict;

use LWP::Simple;
use URI::Escape;

my $data_url = shift;
die "Usage: $0 base_url\n" if @ARGV or not defined $data_url;

my $page_size = 10000;
my $after = "";
while ( 1 ) {
    my $url = $data_url."?mode=list&limit=$page_size&after=".uri_escape($after);
    my $data = get $url;
    last if length($data) == 0;
    print $data;
    my $last = ($data =~ /(?:^|\n)([^\n]+)$/s)[0];
    die unless defined $last;
    $after = $last;
}
