#!/usr/bin/perl
use strict;
use warnings;

use DBI;
use IO::Handle;
use LWP::UserAgent;

my $db = 'furaffinity_recent';
my $dbh = DBI->connect("dbi:mysql:$db", 'ferrox', '');
my $ua = LWP::UserAgent->new;
$ua->agent('Ferrox importer/0.0');

STDOUT->autoflush(1);

my $sth = $dbh->prepare(qq{
    SELECT
        rowid,
        url
    FROM submissions s
    LIMIT 1
});
$sth->execute;

while (my $row = $sth->fetchrow_hashref) {
    my $id = $row->{id};
    my $path = $row->{url};
    my $res = $ua->head('http://d.furaffinity.net/' . $path);

    use Data::Dumper;
    print $res->header('content-length');
}

