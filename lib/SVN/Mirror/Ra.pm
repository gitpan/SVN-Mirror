package SVN::Mirror::Ra;
@ISA = ('SVN::Mirror');
$VERSION = '0.35';
use strict;
use SVN::Core;
use SVN::Repos;
use SVN::Fs;
use SVN::Delta;
use SVN::Ra;
use SVN::Client ();

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    %$self = @_;
    $self->{source} =~ s{/+$}{}g;

    my ($root, $path) = split ('!', $self->{source});

    @{$self}{qw/source source_root source_path/} =
	(join('', $root, $path), $root, $path)
	    if defined $path;

    return $self;
}

sub init_state {
    my ($self, $txn) = @_;
    my $ra = $self->_new_ra;

    my $uuid = $self->{source_uuid} = $ra->get_uuid ();
    my $source_root = $ra->get_repos_root ();
    my $txnroot = $txn->root;

    $txnroot->change_node_prop ($self->{target_path},
				'svm:uuid', "$uuid");

    my $path = $self->{source};
    die "source url not under source root"
	if substr($path, 0, length($source_root), '') ne $source_root;

    $self->{source_root} = $source_root;
    $self->{source_path} = $path;
    $self->{fromrev} = 0;

    return join('!', $source_root, $path);
}

sub load_state {
    my ($self, $txn) = @_;

    my $txnroot = $txn->root;
    my $changed = $self->{root}->node_created_rev ($self->{target_path});

    my $prop = $self->{fs}->revision_proplist ($changed);
    die "no headrev for $self->{source} at rev $changed"
	unless exists $prop->{"svm:headrev:$self->{source}"};
    $self->{fromrev} = $prop->{"svm:headrev:$self->{source}"};
    $self->{VSN} = $prop->{"svm:vsnroot:$self->{source}"};
    # upgrade for 0.27
    $self->{source_uuid} = $txnroot->node_prop ($self->{target_path}, 'svm:uuid');
    my $info = "svm:mirror:$self->{source_uuid}:".($self->{source_path} || '/');

    unless ($txnroot->node_prop ('/', $info)) {
	$txnroot->change_node_prop ('/', $info, $self->{target_path});
	my (undef, $rev) = $txn->commit ();
	$self->{fs}->change_rev_prop ($rev, "svm:headrev:$self->{source}", $self->{fromrev});
	$self->{fs}->change_rev_prop ($rev, "svn:author", 'svm');
	$self->{fs}->change_rev_prop
		    ($rev, "svn:log", "SVM: upgraded info for $self->{target_path}.");
    }
    else {
	$txn->abort ();
    }
}

sub _new_ra {
    my ($self, %arg) = @_;
    SVN::Ra->new( url => $self->{source},
		  auth => $self->{auth},
		  pool => $self->{pool},
		  config => $self->{config},
		  %arg);
}

sub committed {
    my ($self, $date, $sourcerev, $rev, undef, undef, $pool) = @_;
    local $SIG{INT} = 'IGNORE';
    local $SIG{TERM} = 'IGNORE';
    my $cpool = SVN::Pool->new_default ($pool);
    $self->{fs}->change_rev_prop($rev, 'svn:date', $date);
    $self->{fs}->change_rev_prop($rev, "svm:headrev:$self->{source}",
				 "$sourcerev",);
    $self->{fs}->change_rev_prop($rev, "svm:vsnroot:$self->{source}",
				 "$self->{VSN}") if $self->{VSN};
    $self->{headrev} = $rev;

    print "Committed revision $rev from revision $sourcerev.\n";
}

sub mirror {
    my ($self, $fromrev, $paths, $rev, $author, $date, $msg, $ppool) = @_;
    my $ra;

    my $pool = SVN::Pool->new_default ($ppool);
    my $newrev;
    use SVN::Mirror::Ra;
    my $editor = SVN::Mirror::Ra::MirrorEditor->new
	($self->{repos}->get_commit_editor
	 ('', $self->{target_path}, $author, $msg,
	  sub { $newrev = $_[0]; $self->committed($date, $rev, @_) }));

    $self->{working} = $rev;
    $editor->{mirror} = $self;

    $ra = $self->{cached_ra}
	if exists $self->{cached_ra_url} &&
	    $self->{cached_ra_url} eq $self->{source};

    $ra ||= $self->_new_ra ( pool => SVN::Pool->new );
    @{$self}{qw/cached_ra cached_ra_url/} = ($ra, $self->{source});

    if ( ( $fromrev == 0
           || !(defined $fromrev && $self->find_local_rev($fromrev)) )
         && $self->{source} ne $self->{source_root}
       ) {
	(undef, $editor->{anchor}, $editor->{target})
	    = File::Spec->splitpath($editor->{anchor} || $self->{source});
	chop $editor->{anchor};
	$ra = $self->_new_ra ( url => $editor->{anchor}, pool => SVN::Pool->new );
	@{$self}{qw/cached_ra cached_ra_url/} = ($ra, $editor->{anchor});
    }

    $editor->{target} ||= '' if $SVN::Core::VERSION gt '0.36.0';

=begin NOTES

The structure of mod_lists:

* Key is the path of a changed path, a relative path to source_path.
  This is what methods in MirrorEditor get its path, therefore easier
  for them to look up information.

* Value is a hash, containing the following values:

  * action: 'A'dd, 'M'odify, 'D'elete, 'R'eplace
  * remote_path: The path on remote depot
  * remote_rev: The revision on remote depot
  * local_rev:
    * Not Add: -1
    * Add but source is not in local depot: undef
    * Add and source is in local depot: the source revision in local depot
  * local_path: The mapped path of key, ie. the changed path, in local
    depot.
  * local_source_path:
    * Source path is not in local depot: undef
    * Source path is in local depot: a string
  * source_node_kind: Only meaningful if action is 'A'.

=cut

    $editor->{mod_lists} = {};
    my %not_processed;
    foreach ( keys %$paths ) {
        my $item = $paths->{$_};
        my $href;

        my $svn_lpath = my $local_path = $_;
        if ( $editor->{anchor} ) {
            $svn_lpath = $self->{source_root} . $svn_lpath;
            $svn_lpath =~ s|^\Q$editor->{anchor}\E/?||;
            my $source_path = $self->{source_path} || "/";
            $local_path =~ s|^$source_path|$self->{target_path}|;
        } else {
            $svn_lpath =~ s|^$self->{source_path}/?||;
            $local_path = "$self->{target_path}/$svn_lpath";
        }

        my ($action, $rpath, $rrev, $lrev) =
            @$href{qw/action remote_path remote_rev local_rev local_path/} =
                ( $item->action,
                  $item->copyfrom_path,
                  $item->copyfrom_rev,
                  ($item->copyfrom_rev == -1) ?
                    -1 : $self->find_local_rev ($item->copyfrom_rev) || undef,
                  $local_path,
                );

        my ($src_lpath, $source_node_kind) = (undef, $SVN::Node::unknown);
        if ( defined $lrev && $lrev != -1 ) {
            my $rev_root = $self->{fs}->revision_root ($lrev);
            $src_lpath = $rpath;
            $src_lpath =~ s|^$self->{source_path}|$self->{target_path}|;
            $source_node_kind = $rev_root->check_path ($src_lpath);

            if ( $source_node_kind == $SVN::Node::none ) {
                # The source is not in local depot.  Invalidate this
                # copy.
                $href->{local_rev} = undef;
                $src_lpath = undef;
            }
        }
        @$href{qw/local_source_path source_node_kind/} =
            ( $src_lpath, $source_node_kind );

        if ( /^$self->{source_path}/ ) {
            $editor->{mod_lists}{$svn_lpath} = $href;
        } else {
            $not_processed{$svn_lpath} = $href;
        }
    }

    unless (keys %{$editor->{mod_lists}}) {
        print "Skipped revision $rev.\n";
    } else {
        my @mod_list = keys %{$editor->{mod_lists}};
        if ( grep { my $href = $editor->{mod_lists}{$_};
                    !( ( $href->{action} eq 'A'
                         && defined $href->{local_rev}
                         && $href->{local_rev} != -1
                         && $href->{source_node_kind} == $SVN::Node::dir)
                       || $href->{action} eq 'D' )
                } @mod_list ) {
            my $start = $fromrev || ($self->{skip_to} ? $fromrev : $rev-1);
            my $reporter =
                $ra->do_update ($rev, $editor->{target} || '', 1, $editor);
            $reporter->set_path ('', $start, 0);
            $reporter->finish_report ();
        } else {
            # Copies only.  Don't bother fetching full diff through network.
            my $edit = SVN::Simple::Edit->new
                (_editor => [$editor],
                 missing_handler => \&SVN::Simple::Edit::open_missing
                );

            $edit->open_root ($self->{headrev});

            foreach (sort @mod_list) {
                my $href = $editor->{mod_lists}{$_};
                my $action = $href->{action};

                if ($action eq 'A') {
                    $edit->copy_directory ($_, $href->{local_source_path},
                                           $href->{local_rev});
                    $edit->close_directory ($_);
                } elsif ($action eq 'D') {
                    $edit->delete_entry ($_);
                }
            }

            $edit->close_edit ();
        }
    }
    return if defined $self->{mirror}{skip_to} &&
        $self->{mirror}{skip_to} > $rev;

    my $prop;
    $prop = $ra->rev_proplist ($rev) if $self->{revprop};
    for (@{$self->{revprop}}) {
	$self->{fs}->change_rev_prop($newrev, $_, $prop->{$_})
	    if exists $prop->{$_};
    }
}

sub get_merge_back_editor {
    my ($self, $path, $msg, $committed) = @_;
    # get ra commit editor for $self->{source}
    my $ra = $self->_new_ra ( url => "$self->{source}$path" );
    my $youngest_rev = $ra->get_latest_revnum;

    return ($youngest_rev,
	    SVN::Delta::Editor->new ($ra->get_commit_editor ($msg, $committed)));
}

sub run {
    my $self = shift;
    my $startrev = ($self->{skip_to} || 1)-1;
    $startrev = $self->{fromrev}+1 if $self->{fromrev}+1 > $startrev;
    my $endrev = shift || -1;
    my $ra = $self->_new_ra;
    $endrev = $ra->get_latest_revnum () if $endrev == -1;

    print "Syncing $self->{source}\n";

    return unless $endrev == -1 || $startrev <= $endrev;

    print "Retrieving log information from $startrev to $endrev\n";

    $self->{headrev} = $self->{fs}->youngest_rev;
    $ra->get_log ([''], $startrev, $endrev, 1, 1,
		  sub {
		      my ($paths, $rev, $author, $date, $msg, $pool) = @_;
		      # move the anchor detection stuff to &mirror ?
		      if (defined $self->{skip_to} && $rev < $self->{skip_to}) {
			  $author = 'svm';
			  $msg = sprintf('SVM: skipping changes %d-%d for %s',
					 $self->{fromrev}, $rev, $self->{source});
		      }
		      $self->mirror($self->{fromrev}, $paths, $rev, $author,
				    $date, $msg, $pool);
		      $self->{fromrev} = $rev;
		  });
}

package SVN::Mirror::Ra::MirrorEditor;
our @ISA = ('SVN::Delta::Editor');
use strict;
my $debug = 0;

=begin NOTES

1. The path passed to methods in MirrorEditor is a relative directory.
   to $self->{anchor} (if exists) or $self->{mirror}{source_root}.

2. MirrorEditor can fetch usable information in $self->{mod_lists} if
   exists.

3. If we need to call other method in MirrorEditor, the path must be
   in the style of note 1.

4. The diff text passed through network is patch-like.  If a directory
   is copied, there will be one delete_entry() call for the original
   directory, then a LOT of add_directory() and add_file() calls for
   each one of directories and files underneath the new directory.
   This behavior is easy to handle for a real file system, but hard to
   work correct for another subversion reporitory.  A lot of codes,
   especially in add_directory() and add_file(), are specialised for
   handling different conditions.

=cut

sub new {
    my $class = shift;
    my $self = $class->SUPER::new(@_);
    return $self;
}

sub set_target_revision {
    return;
}

sub open_root {
    my ($self, $remoterev, $pool) =@_;
    print "MirrorEditor::open_root($remoterev)\n" if $debug;

    # {copied_paths} stores paths that are copied.  Because there
    # might be copied paths beneath another one, we need it to be an
    # array.
    $self->{copied_paths} = [ ];

    # {visited_paths} keeps track of visited paths.  Parents at the
    # beginning of array, and children the end.  '' means '/'.  $path
    # passed to add_directory() and other methods are in the form of
    # 'deep/path' instead of '/deep/path'.
    $self->{visited_paths} = [ '' ];

    $self->{root} = $self->SUPER::open_root($self->{mirror}{headrev}, $pool);
}

sub open_directory {
    my ($self,$path,$pb,undef,$pool) = @_;
    print "MirrorEditor::open_directory($path)\n" if $debug;
    return undef unless $pb;

    my $dir_baton = $self->SUPER::open_directory ($path, $pb,
                                                  $self->{mirror}{headrev},
                                                  $pool);

    push @{$self->{visited_paths}}, $path;

    if ($self->_under_latest_copypath($path)) {
        $self->_enter_new_copied_path();
        $self->_remove_entries_in_path ($path, $dir_baton, $pool);
    }

    return $dir_baton;
}

sub open_file {
    my ($self,$path,$pb,undef,$pool) = @_;
    print "MirrorEditor::open_file($path)\n" if $debug;
    return undef unless $pb;
    $self->{opening} = $path;
    return $self->SUPER::open_file ($path, $pb,
				    $self->{mirror}{headrev}, $pool);
}

sub change_dir_prop {
    my $self = shift;
    my $baton = shift;
    print "MirrorEditor::change_dir_prop($_[0], $_[1])\n" if $debug;
    # filter wc specified stuff
    return unless $baton;
    return if $_[0] =~ /^svm:/;
    return $self->SUPER::change_dir_prop ($baton, @_)
	unless $_[0] =~ /^svn:(entry|wc):/;
}

sub change_file_prop {
    my $self = shift;
    print "MirrorEditor::change_file_prop($_[1], $_[2])\n" if $debug;
    # filter wc specified stuff
    return unless $_[0];
    return $self->SUPER::change_file_prop (@_)
	unless $_[1] =~ /^svn:(entry|wc):/;
}

# From source to target.  Given a path what svn lib gives, get a path
# where it should be.
sub _translate_rel_path {
    my ($self, $path) = @_;

    if ( exists $self->{mod_lists}{$path} ) {
        return $self->{mod_lists}{$path}{local_path};
    } else {
        if ( $self->{anchor} ) {
            $path = "$self->{anchor}/$path";
            $path =~ s|\Q$self->{mirror}{source_root}\E||;
        } else {
            $path = "$self->{mirror}{source_path}/$path";
        }
        $path =~ s|^$self->{mirror}{source_path}|$self->{mirror}{target_path}|;
        return $path;
    }

}

sub _remove_entries_in_path {
    my ($self, $path, $pb, $pool) = @_;

    foreach ( sort grep $self->{mod_lists}{$_}{action} eq 'D',
              keys %{$self->{mod_lists}} ) {
        next unless m{^$path/([^/]+)$};
        $self->delete_entry ($_, -1, $pb, $pool);
    }
}

# If there's modifications under specified path, return true.
sub _contains_mod_in_path {
    my ($self, $path) = @_;

    foreach ( reverse sort keys %{$self->{mod_lists}} ) {
        return $_
            if index ($_, $path, 0) == 0;
    }

    return;
}

# Given a path, return true if it is a copied path.
sub _is_copy {
    my ($self, $path) = @_;

    return exists $self->{mod_lists}{$path} &&
        $self->{mod_lists}{$path}{remote_path};
}

# Given a path, return source path and revision number in local depot.
sub _get_copy_path_rev {
    my ($self, $path) = @_;

    return ( exists $self->{mod_lists}{$path} ) ?
        @{$self->{mod_lists}{$path}}{qw/local_source_path local_rev/} : ();
}

# Given a path, return true if it's under the lastest visited copied path.
sub _under_latest_copypath {
    my ($self, $path) = @_;

    return (@{$self->{copied_paths}} &&
            index ($path, $self->{copied_paths}[-1]{path}, 0) == 0);
}

# Return undef if not in modified list, action otherwise.
# 'A'dd, 'D'elete, 'R'eplace, 'M'odify
sub _in_modified_list {
    my ($self, $path) = @_;

    if (exists $self->{mod_lists}{$path}) {
        return $self->{mod_lists}{$path}{action};
    } else {
        return;
    }
}

sub _is_under_copy { return scalar @{$_[0]->{copied_paths}} }

# Given a path, return true if the the path is under a copied path
# which has a valid source in local repo.  Otherwise return false.
sub _is_under_true_copy {
    my ($self, $path) = @_;

    return unless scalar @{$self->{copied_paths}};

    $path = $self->{copied_paths}[-1]{path};
    return $self->{mod_lists}{$path}{local_rev};
}

# Given a path, return true if the the path is under a copied path
# which has no valid source in local repo.  Otherwise return false.
sub _is_under_false_copy {
    my ($self, $path) = @_;

    return unless scalar @{$self->{copied_paths}};

    $path = $self->{copied_paths}[-1]{path};
    return !defined ($self->{mod_lists}{$path}{local_source_path});
}

# The following three methods are used to keep tracks of copied paths.
#  _create_new_copied_path($path): call this when you are ready to enter
#      a new copied path.
#  _enter_new_copied_path(): call this when enter a directory under a
#      copied path.  Use _is_under_true_copy() to see if it is true.
#  _leave_new_copied_path(): call this when leave a directory under a
#      copied path.  Use _is_under_true_copy() to see if it is true.
sub _create_new_copied_path {
    my $self = shift;
    my $path = shift;

    push @{$self->{copied_paths}}, { path => $path,
                                     child_depth => 0 };
}

sub _enter_new_copied_path { $_[0]->{copied_paths}[-1]{child_depth}++ }

sub _leave_new_copied_path {
    my $self = shift;

    if ($self->{copied_paths}[-1]{child_depth} == 0) {
        pop @{$self->{copied_paths}};
    } else {
        $self->{copied_paths}[-1]{child_depth}--;
    }
}

sub add_directory {
    my $self = shift;
    my $path = shift;
    my $pb = shift;
    my ($cp_path,$cp_rev,$pool) = @_;
    if ($debug) {
        my ($cp_path, $cp_rev) = $self->_get_copy_path_rev( $path );
        $cp_path = "" unless defined $cp_path;
        $cp_rev = "" unless defined $cp_rev;
        print "MirrorEditor::add_directory($path, $cp_path, $cp_rev)\n";
    }
    return undef unless $pb;

    # rules:
    # in mod_lists, not under copied path:
    #   * A: add_directory()
    #   * M: open_directory()
    #   * R: delete_entry($path), add_directory()
    # under copied path, with local copy source:
    #   * in mod_lists:
    #     A: add_directory()
    #     M: open_directory()
    #     R: delete_entry($path), add_directory()
    #   * not in mod_lists:
    #     * Modifications in the path:
    #       * open_directory().
    #     * No modification in the path:
    #       * Ignore unconditionally.
    # under copied path, without local copy source:
    #   ( add_directory() unconditionally )
    #
    # re-organize:
    # The path is equal to $self->{target}:
    #   * open_directory().
    # under copied path, without local copy source:
    #   * add_directory() unconditionally
    # under copied path, with local copy source, and not in mod_lists:
    #   * Modifications in the path:
    #     * open_directory().
    #   * No modification in the path:
    #     * Ignore unconditionally.
    # in mod_lists:
    #   * A: add_directory()
    #   * M: open_directory()
    #   * R: delete_entry($path), add_directory()
    # else:
    #   raise an error, let others have a chance to give me complains.
    #
    # Rules for 'A', 'M', and 'R':
    # 1. If the path is in the modified list:
    #  action is 'A': 
    #   1a. The source rev is undef, meaning the copy source is not in
    #       local depo.  Pass whatever underneath the path 
    #       unconditionally.
    #   1b. The source rev is an positive integer.  Use add_directory
    #       method.
    #   1c. The source rev is -1, don't tweak @_.  Use add_directory method.
    #  action is 'M':
    #   1d. If the action is 'M', tweak @_ and use open_directory.
    #  action is 'R':
    #   1e. delete the entry first.  If the path has source rev,
    #       supply source path and revision to add_directory.

    my $method = 'add_directory';
    my $is_copy = 0;
    my $action = $self->_in_modified_list ($path);
    if (defined $self->{mirror}{skip_to} &&
        $self->{mirror}{skip_to} > $self->{mirror}{working}) {
        # no-op.
    } elsif ($self->_under_latest_copypath ($path)
            && $self->_is_under_false_copy ($path)) {
        # no-op. add_directory().
        $self->_enter_new_copied_path ();
    } elsif ($self->_under_latest_copypath ($path)
             && $self->_is_under_true_copy ($path)
             && !$action) {
        if ($self->_contains_mod_in_path ($path)) {
            splice @_, 0, 2, $self->{mirror}{headrev};
            $method = 'open_directory';
            $self->_enter_new_copied_path ();
        } else {
            # don't do anything.
            return;
        }
    } elsif ($action) {
        my $item = $self->{mod_lists}{$path};

        if ($action eq 'A') {
            my ($copypath, $copyrev) = $self->_get_copy_path_rev ($path);

            if (!defined($copyrev)) {
                # 1a.
                $self->_create_new_copied_path ($path);
            } elsif ($copyrev == -1) {
                # 1c.
                $self->_enter_new_copied_path ()
                    if $self->_is_under_copy ();
            } else {
                # 1b.
                $is_copy = 1;
                splice (@_, 0, 2, $copypath, $copyrev);

                $self->_create_new_copied_path ($path);
            }
        } elsif ($action eq 'M') {
            # 1d.
            splice @_, 0, 2, $self->{mirror}{headrev};
            $method = 'open_directory';

            $self->_enter_new_copied_path ()
                if $self->_under_latest_copypath ($path);
        } elsif ($action eq 'R') {
            # 1e.
            $self->delete_entry ($path,
                                 $self->{mirror}{headrev},
                                 $pb);

            my ($copypath, $copyrev) = $self->_get_copy_path_rev ($path);
            if (defined ($copyrev) && $copyrev >= 0) {
                $is_copy = 1;
                splice (@_, 0, 2, @$item{qw/local_source_path local_rev/});
                $self->_create_new_copied_path ($path);
            } elsif ( !defined ($copyrev) ) {
                $self->_create_new_copied_path ($path);
            }
        }
    } else {
        # raise error.
        die "Oh no, no more exceptions!  add_directory() failed.";
    }

    if ($path eq $self->{target}) {
        splice @_, 0, 2, $self->{mirror}{headrev}
            if $method ne "open_directory";
        $method = "open_directory";
    }

    push @{$self->{visited_paths}}, $path;
    my $tran_path = $self->_translate_rel_path ($path);

    $method = 'open_directory'
        if $tran_path eq $self->{mirror}{target_path};

    my $dir_baton;
    $method = "SUPER::$method";
    $dir_baton = $self->$method($tran_path, $pb, @_);

    $self->_remove_entries_in_path ($path, $dir_baton, $pool) if $is_copy;

    return $dir_baton;
}

sub apply_textdelta {
    my $self = shift;
    return undef unless $_[0];
    print "MirrorEditor::apply_textdelta($_[0])\n" if $debug;

    $self->SUPER::apply_textdelta (@_);
}

sub close_directory {
    my $self = shift;
    my $baton = shift;
    print "MirrorEditor::close_directory()\n" if $debug;
    return unless $baton;

    my $path = pop @{$self->{visited_paths}};

    $self->_leave_new_copied_path ()
        if $self->_under_latest_copypath ($path);
        
    $self->SUPER::close_directory ($baton);
}

sub close_file {
    my $self = shift;
    print "MirrorEditor::close_file()\n" if $debug;
    return unless $_[0];
    $self->SUPER::close_file(@_);
}

sub add_file {
    my $self = shift;
    my $path = shift;
    my $pb = shift;
    if ($debug) {
        my ($cp_path, $cp_rev) = $self->_get_copy_path_rev( $path );
        $cp_path = "" unless defined $cp_path;
        $cp_rev = "" unless defined $cp_rev;
        print "MirrorEditor::add_file($path, $cp_path, $cp_rev)\n" if $debug;
    }
    return undef unless $pb;

    # rules:
    # 1. If the path is in the modified list:
    #  1a. If the path is a copy, source path and rev must be
    #      specified, and use add_file method. (action could be 'A' or
    #      'R')
    #  1b. If the path is not a copy, don't tweak @_.  Use add_file
    #      method. (action could be 'A' or 'R')
    #  1c. If the action is 'M', tweak @_ and use open_file.
    #  1d. If the action is 'R', delete the entry first.  Then do
    #      proper method.
    # 2. The path is not in the modified list, which means one of its
    #    parent directory must be copied from other place:
    #  2a: If the path is not copied from a directory which exists in
    #      local depot, pass it unconditionally.
    #  2b: If the path is under the latest visited copied path, reject
    #      the file.
    #  2c: Reject the file
    my $method = 'add_file';
    my $action = $self->_in_modified_list ($path);
    if ((defined $self->{mirror}{skip_to}
         && $self->{mirror}{skip_to} > $self->{mirror}{working})
        || ($self->_under_latest_copypath ($path)
            && $self->_is_under_false_copy ($path)) ) {
        # no-op. add_file().
    } elsif ($self->_under_latest_copypath ($path)
             && $self->_is_under_true_copy ($path)
             && !$action) {
        # ignore the path.
        return;
    } elsif ($action) {
        my ($copypath, $copyrev) = $self->_get_copy_path_rev ($path);
        if ( !defined($copyrev) || $copyrev == -1) {
            # 1b. no-op
        } else {
            # 1a.
            splice (@_, 0, 2, $copypath, $copyrev);
        }

        if ($action eq 'M') {
            # 1c.
            splice @_, 0, 2, $self->{mirror}{headrev};
            $method = 'open_file';
        } elsif ($action eq 'R') {
            # 1d.
	    $self->delete_entry ($path, $self->{mirror}{headrev}, $pb);
	    # XXX: why should this fail and ignore the following applytext?
#            return undef;
        }
    } else {
        # raise error.
        die "Oh no, no more exceptions!  add_file() failed.";
    }

    my $tran_path = $self->_translate_rel_path ($path);

    if ($method eq 'add_file') {
        $self->SUPER::add_file ($tran_path, $pb, @_);
    } else {
        $self->SUPER::open_file ($tran_path, $pb, @_);
    }
}

sub delete_entry {
    my ($self, $path, $rev, $pb, $pool) = @_;
    print "MirrorEditor::delete_entry($path, $rev)\n" if $debug;
    return unless $pb;

    my $source_path = $self->{mirror}{source_path};
    my $target_path = $self->{mirror}{target_path};
    $source_path =~ s{^/}{};
    if ($source_path && $path =~ m/^$source_path/) {
        $path =~ s/$source_path/$target_path/;
    } else {
        $path = "$target_path/$path";
    }

    $self->SUPER::delete_entry ($path, $rev, $pb, $pool);
}

sub close_edit {
    my $self = shift;
    print "MirrorEditor::close_edit()\n" if $debug;

    unless ($self->{root}) {
        # If we goes here, this must be an empty revision.  We must
        # replicate an empty revision as well.
        $self->open_root ($self->{mirror}{headrev});
    }

    $self->SUPER::close_directory ($self->{root});
    $self->SUPER::close_edit (@_);
}

1;
