#!/usr/bin/perl
use strict;
use warnings;

use DBI;
use IO::Handle;
use LWP::UserAgent;

my $FAKEMOGILE_DIR = '../ferrox/fakemogile';

my $new = 'furaffinity';
my $old = 'furaffinity_recent';
my $dbh = DBI->connect("dbi:mysql:$new", 'ferrox', '');
my $ua = LWP::UserAgent->new;
$ua->agent('Ferrox importer/0.0');

STDOUT->autoflush(1);

my %update_sths = (
    full => $dbh->prepare(qq{
        UPDATE $new.submissions
        SET mogile_key = ?
        WHERE id = ?
    }),
    half => $dbh->prepare(qq{
        INSERT INTO $new.derived_submissions
            (derivetype, mimetype, mogile_key, submission_id)
        VALUES
            ('halfview', 'image/jpeg', ?, ?)
    }),
    thumb => $dbh->prepare(qq{
        INSERT INTO $new.derived_submissions
            (derivetype, mimetype, mogile_key, submission_id)
        VALUES
            ('thumb', 'image/jpeg', ?, ?)
    }),
);



my $sth = $dbh->prepare(qq{
    SELECT
        s.id,
        s_old.url full,
        s_old.smallerurl half,
        s_old.thumbnail thumb
    FROM $new.submissions s
    INNER JOIN $old.submissions s_old
        ON s.id = s_old.rowid
});
$sth->execute;

while (my $row = $sth->fetchrow_hashref) {
    my $id = $row->{id};
    while (my ($view, $update_sth) = each %update_sths) {
        my $path = $row->{$view};
        next if not $path;

        my $file = $view . $id;
        $ua->get(
            'http://d.furaffinity.net/' . $path,
            ':content_file' => "$FAKEMOGILE_DIR/$file",
        );

        $update_sth->execute($file, $id);
    }

    print "ok: $row->{id}\n";
}

