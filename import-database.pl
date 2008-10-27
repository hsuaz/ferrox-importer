#!/usr/bin/perl

### STATUS
#      df_admin_notes
#      df_adminactions
#      df_administratormessages
#      df_adminmessagereplies
#      df_artistinfo
#      df_comments
# DONE df_favorites
#      df_imageviews
#      df_journal_comments
# DONE df_journals
# DONE df_news
#      df_pageviews
#      df_security_breaches
#      df_shouts
# WONT df_shouts_copy
# WONT df_submission_locks
# PART df_submissions
######   need stats, views, some metadata, and the image itself
# WONT df_suspensions
# ???? df_tmpsubmissions
# DONE df_tracking
#      df_troubletickets
#      df_ttreplies
# WONT df_user_notes
# WONT df_user_params
# ???? df_usermessages
######   perhaps merge with news?
#      df_usermessages_Comments
######   this is messages about comments on submissions; expire old ones or..?
#      df_usermessages_Favorites
######   same concern as above
# DONE df_usermessages_Notes
#      df_usermessages_Shouts
#      df_usermessages_Tickets
#      df_usermessages_Watches
#      df_usermessagesreplies
# PART df_users
######   main user records import; metadata and settings do not
# WONT fx_captcha
# WONT fx_sessions
# WONT fx_user_activation
# WONT fx_useronline
# WONT fx_users
# WONT fx_users_counters
# WONT fx_users_data
# WONT fx_users_temp
#      messagecenter_journals
#      messagecenter_submissions
# WONT submission_filenames
# WONT test
### END STATUS

use strict;
use warnings;

use DBI;
use IO::Handle;

my $new = 'furaffinity';
my $old = 'furaffinity_old';
my $dbh = DBI->connect("dbi:mysql:$new", 'ferrox', '');

STDOUT->autoflush(1);

sub import_data {
    my ($action, $code_ref) = @_;

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
        print "[ ok ]\n";
    }

    return;
}


### Need to use this table to collapse message ids together and keep them
### matched with their corresponding original rows

import_data 'Message table setup' => sub {
    $dbh->do(qq{
        CREATE TEMPORARY TABLE message_ids (
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
    # Use IGNORE here to skip duplicated usernames; we only want the first
    $dbh->do(qq{
        INSERT IGNORE INTO $new.users
            (id, username, email, password, display_name, role_id)
        SELECT
            userid, lower, regemail, userpassword, username,
            CASE accesslevel
                WHEN 0 THEN 2
                WHEN 1 THEN 3
                WHEN 4 THEN 1  -- XXX
                ELSE 1
            END
        FROM $old.df_users
    });
};

# -------------------------------------------------------------------------- #
# Important all sorts of user stuff

import_data 'Watches' => sub {
    die 'Complete';
    $dbh->do(qq{
        INSERT INTO $new.user_relationships
            (from_user_id, to_user_id, relationship)
        SELECT DISTINCT
            user,
            target,
            'watching'
        FROM $old.df_tracking t

        -- ignore deleted users
        INNER JOIN $new.users u_from
            ON t.user = u_from.id
        INNER JOIN $new.users u_to
            ON t.target = u_to.id
    });
};

# TODO remove junk data, like admin blocks or self-blocks?
import_data 'Blocks' => sub {
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
        SELECT userid, blocklist
        FROM $old.df_users
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
        INSERT INTO $new.user_relationships
            (from_user_id, to_user_id, relationship)
        SELECT DISTINCT
            from_user_id,
            to_user_id,
            'blocking'
        FROM $old.blocks
        WHERE to_user_id IS NOT NULL
    });
};
exit;

# -------------------------------------------------------------------------- #

import_data 'News' => sub {
    $dbh->do(qq{
        UPDATE message_ids
        SET other_id = NULL
    });
    $dbh->do(qq{
        INSERT INTO message_ids
            (other_id)
        SELECT rowid
        FROM $old.df_news n

        -- ignore deleted users
        INNER JOIN $new.users u
            ON n.user = u.id
    });

    $dbh->do(qq{
        INSERT INTO $new.messages
            (id, user_id, time, title, content, content_parsed)
        SELECT
            x.id,
            user,
            date,
            subject,
            message,
            message
        FROM $old.df_news n
        INNER JOIN message_ids x
            ON n.rowid = x.other_id
    });
    $dbh->do(qq{
        INSERT INTO $new.news
            (id, message_id, is_anonymous, is_deleted)
        SELECT
            rowid,
            x.id,
            1,
            0
        FROM $old.df_news n
        INNER JOIN message_ids x
            ON n.rowid = x.other_id
    });
};

# XXX reconstruct conversation threads if at all possible
# XXX XXX XXX XXX XXX XXX XXX this is a cool color
# XXX next: then SUBMISSIONS oh gosh
# XXX need to avoid holes in messages table if at all possible...
import_data 'Notes' => sub {
    $dbh->do(qq{
        UPDATE message_ids
        SET other_id = NULL
    });
    $dbh->do(qq{
        INSERT INTO message_ids
            (other_id)
        SELECT rowid
        FROM $old.df_usermessages_Notes n

        -- ignore deleted users
        INNER JOIN $new.users u_from
            ON n.fromlower = u_from.username
        INNER JOIN $new.users u_to
            ON n.targetid = u_to.id
    });

    $dbh->do(qq{
        INSERT INTO $new.messages
            (id, user_id, time, title, content)
        SELECT
            x.id,
            u_from.id,
            n.thisdate,
            n.title,
            n.message
        FROM $old.df_usermessages_Notes n
        INNER JOIN message_ids x
            ON n.rowid = x.other_id
        INNER JOIN $new.users u_from
            ON n.fromlower = u_from.username
    });
    $dbh->do(qq{
        INSERT INTO $new.notes
            (id, message_id, to_user_id, original_note_id, status)
        SELECT
            rowid,
            x.id,
            targetid,
            rowid,
            IF(isread, 'read', 'unread')
        FROM $old.df_usermessages_Notes n
        INNER JOIN message_ids x
            ON n.rowid = x.other_id
    });
};

# XXX comments
import_data 'Journals' => sub {
    $dbh->do(qq{
        UPDATE message_ids
        SET other_id = NULL
    });
    $dbh->do(qq{
        INSERT INTO message_ids
            (other_id)
        SELECT rowid
        FROM $old.df_journals j

        -- ignore deleted users
        INNER JOIN $new.users u
            ON j.user = u.id
    });

    $dbh->do(qq{
        INSERT INTO $new.messages
            (id, user_id, time, title, content, content_parsed)
        SELECT
            x.id,
            user,
            date,
            subject,
            message,
            message
        FROM $old.df_journals j
        INNER JOIN message_ids x
            ON j.rowid = x.other_id
    });
    $dbh->do(qq{
        INSERT INTO $new.journal_entries
            (id, message_id, status)
        SELECT
            rowid,
            x.id,
            'normal'
        FROM $old.df_journals j
        INNER JOIN message_ids x
            ON j.rowid = x.other_id
    });
};

import_data 'User metadata' => sub { die 'todo' };

import_data 'User preferences' => sub { die 'todo' };

import_data 'Submissions' => sub {
    $dbh->do(qq{
        INSERT INTO $new.submissions
            (id, title, description, description_parsed, type, discussion_id, time, status, mogile_key, mimetype, editlog_id)
        SELECT
            rowid,
            title,
            message,
            message,
            CASE category
                WHEN 'music'  THEN 'audio'
                WHEN 'flash'  THEN 'video'
                WHEN 'story'  THEN 'text'
                WHEN 'poetry' THEN 'text'
                ELSE               'image'
            END,
            NULL,  -- XXX discussions
            date,
            'normal',
            '',  -- XXX mogile importing
            '',  -- XXX mimetype
            NULL
        FROM $old.df_submissions
        LIMIT 100
    });
    # XXX take off the limit when this is done
};

import_data 'Favorites' => sub {
    die 'skip';
    $dbh->do(qq{
        INSERT INTO $new.favorite_submissions
            (user_id, submission_id)
        SELECT
            user,
            submissionid
        FROM $old.df_favorites f

        INNER JOIN $new.users u
            ON f.user = u.id
        INNER JOIN $new.submissions s
            ON f.submissionid = s.id
    });
};






# XXX Need to go through all messages and fix them up down here.
# TODO:
# - strip trailing newlines
# - change all newlines to UNIX style
# - parse bbcode!
import_data 'Message formatting' => sub {
    $dbh->do(qq{
        UPDATE $new.messages
        SET
            content_parsed = content,
            content_short = content
    });
};
