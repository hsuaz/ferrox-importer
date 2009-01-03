#!/usr/bin/env perl

use strict;
use warnings;

use DBI;
use IO::Handle;

my $db = 'furaffinity_recent';
my $dbh = DBI->connect("dbi:mysql:$db", 'ferrox', '');
my $sth;

# Rename news's primary key for consistency + sanity
$dbh->do(qq{
    ALTER TABLE news CHANGE rowid row_id INT UNSIGNED NOT NULL
});

### Username fixing

# Tables that contain userids and which columns
# XXX df_* skipped for now
my %user_tables = (
    artistinfo                          => [ 'user_id' ],
    comments_journal                    => [ 'user_id' ],
    comments_submission                 => [ 'user_id' ],
    comments_troubleticket              => [ 'userid' ],
#    df_adminactions                     => [ 'user' ],
#    df_administratormessages            => [ 'userid' ],
#    and replies
    df_security_breaches                => [ 'userid' ],
    df_usermessages                     => [ 'userid' ],
    df_usermessages_Notes               => [ 'targetid' ],
    df_usermessages_Tickets             => [ 'userid' ],
    df_usermessagesreplies              => [ 'userid' ],
    favorites                           => [ 'user_id' ],
    imageviews                          => [ 'user_id' ],
    journals                            => [ 'user_id' ],
    messagecenter_comments_journal      => [ 'user_id' ],
    messagecenter_comments_submission   => [ 'user_id' ],
    messagecenter_favorites             => [ 'user_id' ],
    messagecenter_journals              => [ 'user_id' ],
    messagecenter_shouts                => [ 'user_id' ],
    messagecenter_submissions           => [ 'user_id' ],
    messagecenter_watches               => [ 'user_id' ],
    messagecenter_watches               => [ 'user_id' ],
    news                                => [ 'user' ],
    pageviews                           => [ 'user_id', 'target_id' ],
    shouts                              => [ 'user_id', 'target_id' ],
    troubletickets                      => [ 'userid' ],
    watches                             => [ 'user_id', 'target_id' ],
);

# Find and fix duplicated and deleted users
$sth = $dbh->prepare(qq{
    SELECT
        user_dupe.userid dupe,
        MIN(user_orig.userid) orig
    FROM users user_orig
    INNER JOIN users user_dupe
        ON user_orig.lower = user_dupe.lower
        AND user_orig.userid < user_dupe.userid
    GROUP BY user_dupe.userid
});
$sth->execute;
my %dupe_user_ids;
while (my $row = $sth->fetchrow_hashref) {
    $dupe_user_ids{ $row->{dupe} } = $row->{orig};
}

for my $table (sort keys %user_tables) {
    my $cols_ref = $user_tables{$table};
    for my $col (@{$cols_ref}) {
        # Duplicates
        $sth = $dbh->prepare(qq{
            UPDATE $table
            SET $col = ?
            WHERE $col = ?
        });
        while (my ($dupe, $orig) = each %dupe_user_ids) {
            $sth->execute($orig, $dupe);
        }

        # Deletions
        $dbh->do(qq{
            DELETE $table
            FROM $table
            LEFT JOIN users
                ON $table.$col = users.userid
            WHERE users.userid IS NULL
        });
    }
}
