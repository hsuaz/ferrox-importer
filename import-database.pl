#!/usr/bin/perl

# NEXT UP
# XXX make a $FROM_CLAUSE and $LIMIT_JOIN if $MAX_LIMIT exists, to avoid 
#     needless joining when doing this for real

# PROBLEMS
# - rethreading is ~way too fuckin slow~

### STATUS
#      artistinfo
#           user_id
#           othersite1
#           othersite2
#           othersite3
#           othersite1_link
#           othersite2_link
#           othersite3_link
#           acceptingtrades
#           acceptingrequests
#           acceptingcommissions
#           sketchprices
#           inkprices
#           digitalprices
#           otherprices1
#           otherprices2
#           otherprices1_name
#           otherprices2_name
#           sketchlink
#           inklink
#           digitallink
#           otherprices1link
#           otherprices2link
#           sketchlink_color
#           inklink_color
#           digitallink_color
#           productionque
#           preferedtools
#           history
#           yearsdrawing

#      comments_journal
#           row_id
#           parent_id
#           user_id
#           entity_id
#           subject
#           message
#           date_posted
#           level
#           nest_level

#      comments_submission
#           row_id
#           parent_id
#           user_id
#           entity_id
#           subject
#           message
#           date_posted
#           level
#           nest_level

#      comments_troubleticket
#           rowid
#           ticketid
#           userid
#           isstaff
#           username
#           message
#           date

# SKP? df_adminactions
###### - useful but I don't see the point of keeping old ones?

# SKP? df_administratormessages
# SKP? df_adminmessagereplies
###### - I don't know where these appear and it hasn't been used since 2006

# SKP? df_security_breaches
###### - no dates and don't really apply to the new software

# SKP? df_submission_locks
###### - table is empty

# SKP? df_suspensions
###### - only two rows, seems unused

#      df_usermessages
###### - fold into news
#           rowid
#           userid
#           username
#           subject
#           date
#           replies
#           message

#      df_usermessages_Notes
###### - needs autothreading if possible
#      DONE rowid
#      DONE targetid
#      DONE recipient
#      DONE sender
#      DONE fromlower
#      DONE title
#      DONE thisdate
#      DONE isread
#      DONE message
#           folder

#      df_usermessages_Tickets
###### - not sure how trouble tickets will work

#      df_usermessagesreplies
###### - fold into news comments
#           rowid
#           messageid
#           userid
#           topic
#           username
#           message
#           date

#      favorites
#      SKIP row_id
#      DONE user_id
#      DONE submission_id
#      SKIP category_id
#           date_created

#      imageviews
#           user_id
#           target_id
#           date_viewed

#      journals
#      DONE row_id
#      DONE user_id
#      DONE date_posted
#           num_comments
#      DONE subject
#      DONE message

#      messagecenter_comments_journal
#           user_id
#           entity_id

#      messagecenter_comments_submission
#           user_id
#           entity_id

#      messagecenter_favorites
#           user_id
#           entity_id

#      messagecenter_journals
#           user_id
#           entity_id

#      messagecenter_shouts
#           user_id
#           entity_id

#      messagecenter_submissions
#           user_id
#           entity_id

#      messagecenter_watches
#           user_id
#           entity_id

#      news
#      DONE rowid
#      DONE date
#      DONE user
#           comments
#      SKIP username
#      SKIP lower
#      DONE subject
#      DONE message

#      pageviews
#           user_id
#           target_id
#           date_viewed

# DONE shouts

#      submissions
#      DONE rowid
#      SKIP lock_id
#      DONE date
#      DONE user
#      SKIP username
#      SKIP lower
#      DONE title
#      FRGN url
#      FRGN smallerurl
#      FRGN thumbnail
#           keywords
#      DONE message
#           numtracked
#           comments
#           views
#           width
#           height
#           story
#           poetry
#           category
#           subtype
#           adultsubmission
#           musicfile
#           isscrap
#           gender
#           species
#           tag
#           type
# IRRV submissions_tmp
#      troubletickets
#           rowid
#           userid
#           username
#           issuetype
#           other
#           message
#           resolved
#           lastlookedat
#           replies
#           admin
#           ticketdate
# PART users
#      DONE userid
#      DONE username
#      DONE lower
#           fullname
#      PART userpassword
#           Csid
#           useremail
#      DONE regemail
#           regdate
#           lastvisit
#           lastactivity
#           homepage
#           aim
#           icq
#           yahoo
#           msn
#           biography
#           location
#           interests
#           occupation
#           bdaymonth
#           bdayday
#           bdayyear
#           gender
#           typeartist
#           pageviews
#           mood
#           submissions
#           commentsgiven
#           commentsrecieved
#           shouts
#           favorites
#           journals
#           submissionscount
#           messagescount
#           ip
#           commentcount
#           journalcount
#           submissioncount
#           favoritescount
#           amessagecount
#           notescount
#           watchcount
#           featured
#           shell
#           os
#           quote
#           music
#           favoritemovie
#           favoritegame
#           favoriteplatform
#           favoritemusicplayer
#           favoriteartist
#           favoriteanimal
#           favoritewebsite
#           favoritefood
#           species
#           age
#           seeadultart
#           maturelocked
#           fullview
#           accountlocked
#           lostpw
#           stylefolder
#           stylesheet
#           profileinfo
#      DONE blocklist
#      PART accesslevel
#           journalheader
#           journalfooter
#           siggy
#           hostname
#           last_tmp_submission
#           ttcount
#           suspended
#           timezone
# PART watches
#      IRRV row_id
#      DONE user_id
#      DONE target_id
#           date_watched
#      IRRV watch_type


use strict;
use warnings;

use DBI;
use IO::Handle;

# For sanity and testing purposes, this limits how many notes, submissions,
# and journals are actually imported.  Recent items come before older ones.
my $MAX_ITEMS = 1000;

my $new = 'furaffinity';
my $old = 'furaffinity_recent';
my $dbh = DBI->connect("dbi:mysql:$new", 'ferrox', '');

STDOUT->autoflush(1);

sub completed {
    #die "Complete";
}

sub import_data {
    my ($action, $code_ref) = @_;

    my $start_time = time;
    print '* ', $action, '...';
    eval {
        local $dbh->{PrintError} = 0;
        local $dbh->{RaiseError} = 1;
        $code_ref->();
    };
    my $err = $@;
    
    my $num_spaces = 79
                     - 5  # '* ', '...'
                     - 6  # '[ OK ]'
                     - length $action;
    print ' ' x $num_spaces;
    
    if ($err) {
        print "[FAIL]\n";
        $err =~ s{ (^|\n) (.) }{$1  $2}gmsx;
        print $err;
    }
    else {
        printf "[%3ds]\n", time - $start_time;
    }

    return;
}


sub do_discussions_setup {
    my ($args_ref) = @_;
    my $table = $args_ref->{table};

    $dbh->do(qq{
        UPDATE discussion_ids
        SET other_id = NULL
    });
    $dbh->do(qq{
        INSERT INTO discussion_ids
            (other_id)
        SELECT row_id
        FROM $old.$table t
    });

    $dbh->do(qq{
        INSERT INTO $new.discussions
            (id, comment_count)
        SELECT
            id,
            0
        FROM discussion_ids
        WHERE other_id IS NOT NULL
    });

    return;
}

sub import_comments {
    my ($comments_table, $new_entity_table) = @_;

    my $get_comments_sth = $dbh->prepare(qq{
        SELECT
            c.row_id fa_id,
            c.parent_id parent_id,
            c.user_id user_id,
            c.date_posted time,
            c.subject title,
            c.message content
        FROM $old.$comments_table c
        INNER JOIN $new.users u
            ON c.user_id = u.id
        WHERE c.entity_id = ?
        ORDER BY c.row_id DESC
        LIMIT $MAX_ITEMS
    });
    my $add_comment_sth = $dbh->prepare(qq{
        INSERT INTO $new.comments
            (id, discussion_id, `left`, `right`, user_id, time, title, content)
        VALUES
            (NULL, ?, ?, ?, ?, ?, ?, ?)
    });
    my $entity_sth = $dbh->prepare(qq{
        SELECT id, discussion_id FROM $new.$new_entity_table
    });
    $entity_sth->execute;
    while (my ($id, $discussion_id) = $entity_sth->fetchrow_array) {
        my @tree;
        my %node;
        $get_comments_sth->execute($id);
        while (my $row = $get_comments_sth->fetchrow_hashref) {
            $row->{children} = [];
            $node{ $row->{fa_id} } = $row;

            if ($row->{parent_id}) {
                push @{ $node{ $row->{parent_id} }{children} }, $row;
            }
            else {
                push @tree, $row;
            }
        }

        create_adjacency_list(\(my $anon = 1), @tree);

        for my $id (sort keys %node) {
            # XXX Early on, FA apparently allowed guests to reply to things
            # under certain circumstances, so there are a handful of comments
            # that have a user_id of 0, so they're skipped by the INNER JOIN
            # to the users table, so they're orphaned from the nodes in @tree,
            # so they never got left/right assigned.  We can't insert rows
            # with no matching user, so for now we're just discarding the
            # orphan comments; if Ferrox supports deleted comments before
            # release (i.e. no message row), we can just mark these bogus
            # comments as deleted.  Note that in many cases the correct user
            # logged in and reposted them, so orphaned guest comments might
            # be scrappable entirely.
            next if not defined $node{$id}{left};

            $add_comment_sth->execute(
                $discussion_id,
                @{ $node{$id} }{qw/
                    left right
                    user_id time title content
                /}
            );
        }
    }
}

sub create_adjacency_list {
    my ($n_ref, @nodes) = @_;
    for my $node (@nodes) {
        $node->{left} = ${$n_ref};
        ${$n_ref}++;

        create_adjacency_list($n_ref, @{ $node->{children} });

        $node->{right} = ${$n_ref};
        ${$n_ref}++;
    }
}



### Clear out existing tables
import_data 'Truncating' => sub {
    my @tables = qw/
        user_relationships
        news
        notes
        journal_entries
        favorite_submissions
        user_submissions
        derived_submissions
        submissions
        comments
        discussions
    /;

    for my $table (@tables) {
        $dbh->do(qq{
            TRUNCATE $new.$table
        });
    }
};

### Need to use this table to collapse message ids together and keep them
### matched with their corresponding original rows

import_data 'Temporary table setup' => sub {
    $dbh->do(qq{
        CREATE TEMPORARY TABLE message_ids (
            id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
            other_id INT UNSIGNED UNIQUE
        )
    });
    $dbh->do(qq{
        CREATE TEMPORARY TABLE discussion_ids (
            id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
            other_id INT UNSIGNED UNIQUE
        )
    });
};

### Users

# Arbitrarily-defined roles:
#   old 0 == new 2: regular user
#   old 1 == new 3: admin
#   old 4 == new X: banned
# If you change these here, CHANGE THEM BELOW TOO!
import_data 'Roles' => sub {
    die 'skip for now; websetup should do this';
    $dbh->do(qq{
        INSERT INTO $new.roles
            (id, name, sigil, description)
        VALUES
            (1, 'XXX', '-', 'Banned?  Or something'),
            (2, 'Member', '~', 'Regular user'),
            (3, 'Admin', '\@', 'Superpowers activated')
    });
};

import_data 'Users' => sub {
    completed;
    # Use IGNORE here to skip duplicated usernames; we only want the first
    # XXX use names or something
    $dbh->do(qq{
        INSERT IGNORE INTO $new.users
            (id, username, email, password, display_name, role_id)
        SELECT
            userid, lower, regemail, userpassword, username,
            CASE accesslevel
                WHEN 0 THEN 5
                WHEN 1 THEN 6
                WHEN 4 THEN 4
                ELSE 5
            END
        FROM $old.users
    });
};

# -------------------------------------------------------------------------- #
# Important all sorts of user stuff

import_data 'Watches' => sub {
    die "alright this takes AGES seriously";
    completed;
    $dbh->do(qq{
        INSERT IGNORE INTO $new.user_relationships
            (from_user_id, to_user_id, relationship)
        SELECT
            user_id,
            target_id,
            'watching'
        FROM $old.watches t
    });
};

# TODO remove junk data, like admin blocks or self-blocks?
import_data 'Blocks' => sub {
    completed;
    # Some intermediate storage
    $dbh->do(qq{
        CREATE TEMPORARY TABLE $old.blocks (
            from_user_id INT UNSIGNED NOT NULL,
            to_user_id INT UNSIGNED NULL,
            to_username VARCHAR(64) NOT NULL
        )
    });

    # And a query
    my $insert_sth = $dbh->prepare(qq{
        INSERT INTO $old.blocks
            (from_user_id, to_user_id, to_username)
        VALUES (?, NULL, ?)
    });

    # Fuck
    my $sth = $dbh->prepare(qq{
        SELECT u.userid, u.blocklist
        FROM $old.users u
        INNER JOIN $new.users u2
            ON u.userid = u2.id
        WHERE blocklist != ""
    });
    $sth->execute;
    while (my $row = $sth->fetchrow_hashref) {
        my @blocked_users = split / [\x0a\x0d]+ /x, $row->{blocklist};
        for my $username (@blocked_users) {
            # Trim extra whitespace and skip if there's nothing left
            $username =~ s{ \A \s+ | \s+ \z }{}x;
            next if not $username;

            # Delete underscores so we can compare unambiguously to username
            $username =~ s{_}{}g;

            # Try to fix some common errors; these blocks didn't actually work
            # on old FA, but it seems polite to at least try to guess what on
            # earth people meant
            $username =~ s{\Ahttp://www.furaffinity.net/user/}{};  # URLs
            $username =~ s{\A~}{};  # sigils
            $username =~ s{\A \* \s+}{}x;  # some people make ascii lists...

            $insert_sth->execute($row->{userid}, $username);
        }
    }

    # Find the blocked users' ids
    $dbh->do(qq{
        UPDATE $old.blocks b
        INNER JOIN $new.users u
            ON b.to_username = u.username
        SET b.to_user_id = u.id
    });

    # Copy to new table
    $dbh->do(qq{
        -- Some people have duplicate blocks...
        INSERT IGNORE INTO $new.user_relationships
            (from_user_id, to_user_id, relationship)
        SELECT
            from_user_id,
            to_user_id,
            'blocking'
        FROM $old.blocks
        WHERE to_user_id IS NOT NULL
    });
};

import_data 'Shouts' => sub {
    do_discussions_setup({
        table => 'users',
    });

    # left and right need to just increment within the same user to make each
    # shout the root of its own comment tree:
    # 1 shout1 2   3 shout2 4  5 shout3 6
    # To do this, we start a counter, increment it with each insertion, and
    # reset it to zero when we start on a new user.  Have to do the user_id
    # column AFTER left/right!
    $dbh->do(q{ SET @last_user = 0 });
    $dbh->do(q{ SET @leftright = 0 });
    $dbh->do(qq{
        INSERT INTO $new.comments
            (id, discussion_id, `left`, `right`, user_id, time, title, content)
        SELECT
            NULL,
            x.id,
            IF(user_id = \@last_user, \@leftright := \@leftright + 1, \@leftright = 1),
            (\@leftright := \@leftright + 1),
            (\@last_user := user_id),
            date_posted,
            '',
            message
        FROM $old.shouts s
        INNER JOIN discussion_ids x
            ON s.target_id = x.other_id
        ORDER BY s.user_id
    });
};

# -------------------------------------------------------------------------- #

import_data 'News' => sub {
    do_discussions_setup({
        table => 'news',
    });

    $dbh->do(qq{
        INSERT INTO $new.news
            (id, discussion_id, is_anonymous, is_deleted, user_id, time, title, content)
        SELECT
            row_id,
            x.id,
            1,
            0,
            user,
            date,
            subject,
            message
        FROM $old.news n
        INNER JOIN $new.users u
            ON n.user = u.id
        INNER JOIN discussion_ids x
            ON n.row_id = x.other_id
        ORDER BY n.row_id DESC
        LIMIT $MAX_ITEMS
    });
};

# XXX should original_note_id always be == id here?
import_data 'Notes' => sub {
    $dbh->do(qq{
        UPDATE message_ids
        SET other_id = NULL
    });

    $dbh->do(qq{
        INSERT INTO $new.notes
            (id, to_user_id, original_note_id, status, from_user_id, time, title, content)
        SELECT
            rowid,
            targetid,
            rowid,
            IF(isread, 'read', 'unread'),
            u_from.id,
            n.thisdate,
            n.title,
            n.message
        FROM $old.df_usermessages_Notes n
        INNER JOIN $new.users u_to
            ON n.targetid = u_to.id
        INNER JOIN $new.users u_from
            ON n.fromlower = u_from.username
        ORDER BY n.rowid DESC
        LIMIT $MAX_ITEMS
    });
};

import_data 'Journals' => sub {
    do_discussions_setup({
        table => 'journals',
    });

    $dbh->do(qq{
        INSERT INTO $new.journal_entries
            (id, discussion_id, status, user_id, time, title, content)
        SELECT
            row_id,
            x.id,
            'normal',
            user_id,
            date_posted,
            subject,
            message
        FROM $old.journals j
        INNER JOIN $new.users u
            ON j.user_id = u.id
        INNER JOIN discussion_ids x
            ON j.row_id = x.other_id
        ORDER BY j.row_id DESC
        LIMIT $MAX_ITEMS
    });
};

import_data 'Journal comments' => sub {
    import_comments('comments_journal', 'journal_entries');
};

################################################################################

import_data 'User metadata' => sub { die 'todo' };

import_data 'User preferences' => sub { die 'todo' };

# XXX comments
import_data 'Submissions' => sub {
    do_discussions_setup({
        table => 'submissions',
    });

    $dbh->do(qq{
        INSERT INTO $new.submissions
            (id, type, discussion_id, time, title, status, mogile_key, mimetype)
        SELECT
            row_id,
            CASE category
                WHEN 'music'  THEN 'audio'
                WHEN 'flash'  THEN 'video'
                WHEN 'story'  THEN 'text'
                WHEN 'poetry' THEN 'text'
                ELSE               'image'
            END,
            y.id,
            date,
            title,
            'normal',
            '',
            ''  -- XXX mimetype
        FROM $old.submissions s
        INNER JOIN $new.users u
            ON s.user = u.id
        INNER JOIN discussion_ids y
            ON s.row_id = y.other_id
        ORDER BY s.row_id DESC
        LIMIT $MAX_ITEMS
    });

    # Artist association
    $dbh->do(qq{
        INSERT INTO $new.user_submissions
            (user_id, submission_id, relationship, ownership_status, time, content)
        SELECT
            s.user,
            s.row_id,
            'artist',
            'primary',
            date,
            message
        FROM $old.submissions s
        INNER JOIN $new.submissions s2
            ON s.row_id = s2.id
    });
};

import_data 'Favorites' => sub {
    $dbh->do(qq{
        -- There are, somehow, dupes in the source data
        INSERT IGNORE INTO $new.favorite_submissions
            (user_id, submission_id)
        SELECT
            user_id,
            submission_id
        FROM $old.favorites f
        INNER JOIN $new.submissions s
            ON f.submission_id = s.id
    });
};

import_data 'Submission comments' => sub {
    import_comments('comments_submission', 'submissions');
};



import_data 'Comment count' => sub {
    $dbh->do(qq{
        UPDATE discussions d
        INNER JOIN (
            SELECT discussion_id, COUNT(*) ct
            FROM comments
            GROUP BY discussion_id
        ) c
            ON c.discussion_id = d.id
        SET comment_count = c.ct;
    });
};



# XXX Need to go through all messages and fix them up down here.
# TODO:
# - strip trailing newlines
# - change all newlines to UNIX style
# - parse bbcode!
import_data 'Message formatting' => sub {
    die "nop";

    for my $message_table (qw( news notes journals submissions comments )) {
        $dbh->do(qq{
            UPDATE $new.$message_table
            SET
                foo = bar
            WHERE id = something
        });
    }
};
