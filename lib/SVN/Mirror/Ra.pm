package SVN::Mirror::Ra;
@ISA = ('SVN::Mirror');
$VERSION = '0.67';
use strict;
use SVN::Core;
use SVN::Repos;
use SVN::Fs;
use SVN::Delta;
use SVN::Ra;
use SVN::Client ();
use constant OK => $SVN::_Core::SVN_NO_ERROR;

sub new {
    my $class = shift;
    my $self = bless {}, $class;
    %$self = @_;
    $self->{source} =~ s{/+$}{}g;

    @{$self}{qw/source source_root source_path/} =
	_parse_source ($self->{source});

    @{$self}{qw/rsource rsource_root rsource_path/} =
	_parse_source ($self->{rsource}) if $self->{rsource};

    return $self;
}

sub _parse_source {
    my $source = shift;
    my ($root, $path) = split ('!', $source, 2);
    $path ||= '';
    return (join('', $root, $path), $root, $path)
}

sub _store_source {
    my ($root, $path) = @_;
    return join('!', $root, $path);
}

sub _get_prop {
    my ($self, $ra, $path, $propname) = @_;
}

sub _is_descendent {
    my ($parent, $child) = @_;
    return 1 if $parent eq $child;
    $parent = "$parent/" unless $parent eq '/';
    return $parent eq substr ($child, 0, length ($parent));
}

sub _check_overlap {
    my ($self) = @_;
    my $fs = $self->{repos}->fs;
    my $root = $fs->revision_root ($fs->youngest_rev);
    for (map {$root->node_prop ($_, 'svm:source')} SVN::Mirror::list_mirror ($self->{repos})) {
	my (undef, $source_root, $source_path) = _parse_source ($_);
	next if $source_root ne $self->{source_root};
	die "Mirroring overlapping paths not supported\n"
	    if _is_descendent ($source_path, $self->{source_path})
	    || _is_descendent ($self->{source_path}, $source_path);
    }
}

sub init_state {
    my ($self, $txn) = @_;
    my $ra = $self->_new_ra (url => $self->{source});

    my $uuid = $self->{source_uuid} = $ra->get_uuid ();
    my $source_root = $ra->get_repos_root ();
    my $path = $self->{source};
    $txn->abort, die "source url not under source root"
	if substr($path, 0, length($source_root), '') ne $source_root;

    $self->{source_root} = $source_root;
    $self->{source_path} = $path;
    $self->{fromrev} = 0;

    # XXX: abort txn before dying
    $self->_check_overlap;

    # check if the url exists
    if ($ra->check_path ('', -1) != $SVN::Node::dir) {
	$txn->abort;
	die "$self->{source} is not a directory.\n";
    }
    unless ($self->{source} eq $self->{source_root}) {
	undef $ra; # bizzare perlgc
	$ra = $self->_new_ra (url => $self->{source_root});
    }

    # check if mirror source is already a mirror
    # older SVN::RA will return Reporter so prop would be undef
    my (undef, undef, $prop) = $ra->get_dir ('', -1);
    if ($prop && $prop->{'svm:mirror'}) {
	my $rroot;
	for ($prop->{'svm:mirror'} =~ m/^.*$/mg) {
	    if (_is_descendent ($_, $self->{source_path})) {
		$rroot = $_;
		last;
	    }
	    elsif (_is_descendent ($self->{source_path}, $_)) {
		$txn->abort, die "Can't relay mirror outside mirror anchor $_";
	    }
	}
	if ($rroot) {
	    $rroot =~ s|^/||;
	    (undef, undef, $prop) = $ra->get_dir ($rroot, -1);
	    $txn->abort, die "relayed mirror source doesn't not have svm:source"
		unless exists $prop->{'svm:source'};
	    @{$self}{qw/rsource rsource_root rsource_path/} =
		@{$self}{qw/source source_root source_path/};
	    $self->{rsource_uuid} = $uuid;
	    $self->{source_path} =~ s|^/\Q$rroot\E||;
	    @{$self}{qw/source source_uuid/} = @{$prop}{qw/svm:source svm:uuid/};
	    $self->{source} .= '!' if index ($self->{source}, '!') == -1;
	    @{$self}{qw/source source_root source_path/} =
		_parse_source ($self->{source}.$self->{source_path});

	    $txn->abort, die "relayed source and source have same repository uuid"
		if $self->{source_uuid} eq $self->{rsource_uuid};

	    my $txnroot = $txn->root;
	    $txnroot->change_node_prop ($self->{target_path}, 'svm:rsource',
					_store_source ($source_root, $path));
	    $txnroot->change_node_prop ($self->{target_path}, 'svm:ruuid',
					$uuid);
	    $txn->change_prop ("svm:headrev", "$self->{rsource_uuid}:$self->{fromrev}\n");

	    return _store_source ($self->{source_root}, $self->{source_path});
	}
    }

    @{$self}{qw/rsource rsource_root rsource_path/} =
	@{$self}{qw/source source_root source_path/};

    $self->{rsource_uuid} = $self->{source_uuid};

    $txn->change_prop ("svm:headrev", "$self->{rsource_uuid}:$self->{fromrev}\n");
    return _store_source ($source_root, $path);
}

sub load_state {
    my ($self) = @_;

    my $prop = $self->{root}->node_proplist ($self->{target_path});
    @{$self}{qw/source_uuid rsource_uuid/} =
	@{$prop}{qw/svm:uuid svm:ruuid/};
    unless ($self->{rsource}) {
	@{$self}{qw/rsource rsource_root rsource_path/} =
	    @{$self}{qw/source source_root source_path/};
	$self->{rsource_uuid} = $self->{source_uuid};
    }

    die "please upgrade the mirror state\n"
	if $self->{root}->node_prop ('/', join (':', 'svm:mirror', $self->{source_uuid},
						$self->{source_path} || '/'));

    unless ($self->{ignore_lock}) {
	die "no headrev"
	    unless defined $self->load_fromrev;
    }
    return;
}

sub _new_ra {
    my ($self, %arg) = @_;
    $self->{config} ||= SVN::Core::config_get_config (undef, $self->{pool});
    $self->{auth} ||= $self->_new_auth;
    SVN::Ra->new( url => $self->{rsource},
		  auth => $self->{auth},
		  config => $self->{config},
		  %arg);
}

sub _new_auth {
    my ($self) = @_;
    # create a subpool that is not automatically destroyed
    my $pool = SVN::Pool::create (${$self->{pool}});
    $pool->default;
    my ($baton, $ref) = SVN::Core::auth_open_helper([
        SVN::Client::get_simple_provider (),
        SVN::Client::get_ssl_server_trust_file_provider (),
        SVN::Client::get_username_provider (),
        SVN::Client::get_simple_prompt_provider( $self->can('_simple_prompt'), 2),
        SVN::Client::get_ssl_server_trust_prompt_provider( $self->can('_ssl_server_trust_prompt') ),
        SVN::Client::get_ssl_client_cert_prompt_provider( $self->can('_ssl_client_cert_prompt'), 2 ),
        SVN::Client::get_ssl_client_cert_pw_prompt_provider( $self->can('_ssl_client_cert_pw_prompt'), 2 ),
        SVN::Client::get_username_prompt_provider( $self->can('_username_prompt'), 2),
    ]);
    $self->{auth_ref} = $ref;
    return $baton;
}

sub _simple_prompt {
    my ($cred, $realm, $default_username, $may_save, $pool) = @_;

    if (defined $default_username and length $default_username) {
        print "Authentication realm: $realm\n" if defined $realm and length $realm;
        $cred->username($default_username);
    }
    else {
        _username_prompt($cred, $realm, $may_save, $pool);
    }

    $cred->password(_read_password("Password for '" . $cred->username . "': "));
    $cred->may_save($may_save);

    return OK;
}

sub _ssl_server_trust_prompt {
    my ($cred, $realm, $failures, $cert_info, $may_save, $pool) = @_;

    print "Error validating server certificate for '$realm':\n";

    print " - The certificate is not issued by a trusted authority. Use the\n",
          "   fingerprint to validate the certificate manually!\n"
      if ($failures & $SVN::Auth::SSL::UNKNOWNCA);

    print " - The certificate hostname does not match.\n"
      if ($failures & $SVN::Auth::SSL::CNMISMATCH);

    print " - The certificate is not yet valid.\n"
      if ($failures & $SVN::Auth::SSL::NOTYETVALID);

    print " - The certificate has expired.\n"
      if ($failures & $SVN::Auth::SSL::EXPIRED);

    print " - The certificate has an unknown error.\n"
      if ($failures & $SVN::Auth::SSL::OTHER);

    printf(
        "Certificate information:\n".
        " - Hostname: %s\n".
        " - Valid: from %s until %s\n".
        " - Issuer: %s\n".
        " - Fingerprint: %s\n",
        map $cert_info->$_, qw(hostname valid_from valid_until issuer_dname fingerprint)
    );

    print(
        $may_save
            ? "(R)eject, accept (t)emporarily or accept (p)ermanently? "
            : "(R)eject or accept (t)emporarily? "
    );

    my $choice = lc(substr(<STDIN> || 'R', 0, 1));

    if ($choice eq 't') {
        $cred->may_save(0);
        $cred->accepted_failures($failures);
    }
    elsif ($may_save and $choice eq 'p') {
        $cred->may_save(1);
        $cred->accepted_failures($failures);
    }

    return OK;
}

sub _ssl_client_cert_prompt {
    my ($cred, $realm, $may_save, $pool) = @_;

    print "Client certificate filename: ";
    chomp(my $filename = <STDIN>);
    $cred->cert_file($filename);

    return OK;
}

sub _ssl_client_cert_pw_prompt {
    my ($cred, $realm, $may_save, $pool) = @_;

    $cred->password(_read_password("Passphrase for '%s': "));

    return OK;
}

sub _username_prompt {
    my ($cred, $realm, $may_save, $pool) = @_;

    print "Authentication realm: $realm\n" if defined $realm and length $realm;
    print "Username: ";
    chomp(my $username = <STDIN>);
    $username = '' unless defined $username;

    $cred->username($username);

    return OK;
}

sub _read_password {
    my ($prompt) = @_;

    print $prompt;

    require Term::ReadKey;
    Term::ReadKey::ReadMode('noecho');

    my $password = '';
    while (defined(my $key = Term::ReadKey::ReadKey(0))) {
        last if $key =~ /[\012\015]/;
        $password .= $key;
    }

    Term::ReadKey::ReadMode('restore');
    print "\n";

    return $password;
}

sub _revmap {
    my ($self, $rev, $ra) = @_;
    $ra ||= $self->{cached_ra};
    $SVN::Core::VERSION ge '1.1.0' ?
	$ra->rev_prop ($rev, 'svm:headrev') :
	$ra->rev_proplist ($rev)->{'svm:headrev'};
}

sub committed {
    my ($self, $revmap, $date, $sourcerev, $rev) = @_;
    $self->{fs}->change_rev_prop($rev, 'svn:date', $date);
    # sync remote headrev too
    $self->{fs}->change_rev_prop($rev, 'svm:headrev', $revmap."$self->{rsource_uuid}:$sourcerev\n");
    $self->{fs}->change_rev_prop($rev, 'svm:incomplete', '*')
	if $self->{rev_incomplete};
    $self->{headrev} = $rev;

    $self->unlock ('mirror');
    print "Committed revision $rev from revision $sourcerev.\n";
}

our $debug;

sub mirror {
    my ($self, $fromrev, $paths, $rev, $author, $date, $msg, $ppool) = @_;
    my $ra;

    if ($debug and eval { require BSD::Resource; 1 }) {
	my ($usertime, $systemtime,
	    $maxrss, $ixrss, $idrss, $isrss, $minflt, $majflt, $nswap,
	    $inblock, $oublock, $msgsnd, $msgrcv,
	    $nsignals, $nvcsw, $nivcsw) = BSD::Resource::getrusage();
	print ">>> mirroring $rev:\n";
	print ">>> $usertime $systemtime $maxrss $ixrss $idrss $isrss\n";
    }

    my $pool = SVN::Pool->new_default ($ppool);
    my ($newrev, $revmap);

    $ra = $self->{cached_ra}
	if exists $self->{cached_ra_url} &&
	    $self->{cached_ra_url} eq $self->{rsource};
    if ($ra && $self->{rsource} =~ m/^http/ && --$self->{cached_life} == 0) {
	undef $ra;
    }
    $ra ||= $self->_new_ra;

    $revmap = $self->_revmap ($rev, $ra) if $self->_relayed;
    $revmap ||= '';

    my $editor = SVN::Mirror::Ra::MirrorEditor->new
	($self->{repos}->get_commit_editor
	 ('', $self->{target_path}, $author, $msg,
	  sub { $newrev = $_[0];
		$self->committed ($revmap, $date, $rev, @_) }));

    $self->{working} = $rev;
    $editor->{mirror} = $self;

    @{$self}{qw/cached_ra cached_ra_url/} = ($ra, $self->{rsource});
    if ( ( $fromrev == 0
# WTF do we need to check this?
#           || !(defined $fromrev && $self->find_local_rev($fromrev, $self->{rsource_uuid}))
         )
         && $self->{rsource} ne $self->{rsource_root}
       ) {
	(undef, $editor->{anchor}, $editor->{target})
	    = File::Spec::Unix->splitpath($editor->{anchor} || $self->{rsource});
	chop $editor->{anchor};
	$ra = $self->_new_ra ( url => $editor->{anchor} );
	undef $self->{cached_ra}; # bizzare perlgc
	@{$self}{qw/cached_ra cached_ra_url/} = ($ra, $editor->{anchor});
    }
    $self->{cached_life} ||= 100;
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
    foreach ( keys %$paths ) {
        my $item = $paths->{$_};
	s/\n/ /g; # XXX: strange edge case
        my $href;

        my $svn_lpath = my $local_path = $_;
        if ( $editor->{anchor} ) {
            $svn_lpath = $self->{rsource_root} . $svn_lpath;
            $svn_lpath =~ s|^\Q$editor->{anchor}\E/?||;
            my $source_path = $self->{rsource_path} || "/";
            $local_path =~ s|^\Q$source_path\E|$self->{target_path}|;
        } else {
            $svn_lpath =~ s|^\Q$self->{rsource_path}\E/?||;
            $local_path = "$self->{target_path}/$svn_lpath";
        }

	my $local_rev = -1;
	unless ($item->copyfrom_rev == -1) {
	    $local_rev = $self->find_local_rev
		($item->copyfrom_rev, $self->{rsource_uuid});
	}
	# XXX: the logic of the code here is a mess!
        my ($action, $rpath, $rrev, $lrev) =
            @$href{qw/action remote_path remote_rev local_rev local_path/} =
                ( $item->action,
                  $item->copyfrom_path,
                  $item->copyfrom_rev,
		  $local_rev,
                  $local_path,
                );
	# workaround fsfs remoet_path inconsistencies
	$rpath = "/$rpath" if $rpath && substr ($rpath, 0, 1) ne '/';
        my ($src_lpath, $source_node_kind) = (undef, $SVN::Node::unknown);
	# XXX: should check if the copy is within the anchor before resolving lrev
        if ( defined $lrev && $lrev != -1 ) {
	    $src_lpath = $rpath;
	    # copy within mirror anchor
            if ($src_lpath =~ s|^\Q$self->{rsource_path}\E/|$self->{target_path}/|) {
		# $source_node_kind is used for deciding if we need reporter later
		my $rev_root = $self->{fs}->revision_root ($lrev);
		$source_node_kind = $rev_root->check_path ($src_lpath);
	    }
	    else {
		($src_lpath, $href->{local_rev}) = (undef, undef);
	    }
	}
	elsif ($rrev != -1) {
	    # The source is not in local depot.  Invalidate this
	    # copy.
	    ($src_lpath, $href->{local_rev}) =
		$self->{cb_copy_notify}
		? $self->{cb_copy_notify}->($self, $local_path, $rpath, $rrev)
		: (undef, undef)
        }
        @$href{qw/local_source_path source_node_kind/} =
            ( $src_lpath, $source_node_kind );

	# XXX: the loop should not reached here if changed path is
	# not interesting to us, skip them at the beginning the the loop
        if ( $_ eq $self->{rsource_path} or
	     index ("$_/", "$self->{rsource_path}/") == 0 ) {
            $editor->{mod_lists}{$svn_lpath} = $href;
        } elsif ($rrev != -1 && $href->{action} eq 'A' &&
		 index ($self->{rsource_path}, "$_/") == 0) {
	    # special case for the parent of the anchor is copied.
	    my $reanchor = $self->{rsource_path};
	    $reanchor =~ s{^\Q$_\E/}{};
	    $href->{remote_path} .= '/'.$reanchor;
	    $href->{local_path} = $self->{target_path};
            $editor->{mod_lists}{length $svn_lpath ? "$svn_lpath/$reanchor"
				     : $reanchor} = $href;
        }
    }

    unless (keys %{$editor->{mod_lists}}) {
	my $root = $editor->open_root($self->{headrev});
	$editor->change_dir_prop ($root, svm => undef);
	$editor->close_directory($root);
	$editor->close_edit;
    } else {
        my @mod_list = sort keys %{$editor->{mod_lists}};
	# mark item as directory that we are sure about.
	# do not use !isdir for deciding the item is _not_ a directory.
	for my $parent (@mod_list) {
	    for (@mod_list) {
		next if $parent eq $_;
		if (index ("$_/", "$parent/") == 0) {
		    $editor->{mod_lists}{$parent}{isdir} = 1;
		    last;
		}
	    }
	}
        if ( ($self->{skip_to} && $self->{skip_to} <= $rev) ||
	     grep { my $href = $editor->{mod_lists}{$_};
                    !( ( ($href->{action} eq 'A' || $href->{action} eq 'R')
                         && ((defined $href->{local_rev}
			      && $href->{local_rev} != -1
			      && $href->{source_node_kind} == $SVN::Node::dir)
			     || ($href->{isdir})
			    ))
                       || $href->{action} eq 'D' )
                } @mod_list ) {
	    my $pool = SVN::Pool->new_default_sub;

            my $start = $fromrev || ($self->{skip_to} ? $fromrev : $rev-1);
            my $reporter =
                $ra->do_update ($rev, $editor->{target} || '', 1, $editor);
	    my @lock = $SVN::Core::VERSION ge '1.2.0' ? (undef) : ();
            $reporter->set_path ('', $start, @lock, 0);

            $reporter->finish_report ();
        } else {
            # Copies only.  Don't bother fetching full diff through network.
            my $edit = SVN::Simple::Edit->new
                (_editor => [$editor],
                 missing_handler => \&SVN::Simple::Edit::open_missing
                );

            $edit->open_root ($self->{headrev});

            foreach (@mod_list) {
                my $href = $editor->{mod_lists}{$_};
                my $action = $href->{action};

		if ($action eq 'D' || $action eq 'R') {
                    $edit->delete_entry ($_);
                }

                if ($action eq 'A' || $action eq 'R') {
		    if (defined $href->{local_rev} && $href->{local_rev} != -1) {
			$edit->copy_directory ($_, $href->{local_source_path},
					       $href->{local_rev});
		    }
		    else {
			$edit->add_directory($_);
		    }
                    $edit->close_directory ($_);
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

sub _relayed { $_[0]->{rsource} ne $_[0]->{source} }

sub get_merge_back_editor {
    my ($self, $path, $msg, $committed) = @_;
    die "relayed merge back not supported yet" if $self->_relayed;
    @{$self}{qw/cached_ra cached_ra_url/} =
	($self->_new_ra ( url => "$self->{source}$path"), "$self->{source}$path" );

    $self->{commit_ra} = $self->{cached_ra};
    $self->load_fromrev;
    my @lock = $SVN::Core::VERSION ge '1.2.0' ? (undef, 0) : ();
    return ($self->{fromrev}, SVN::Delta::Editor->new
	    ($self->{cached_ra}->get_commit_editor ($msg, $committed, @lock)));
}

sub switch {
    my ($self, $url) = @_;
    my $ra = $self->_new_ra (url => $url);
    # XXX: get proper uuid like init_state
    die "uuid is different" unless $ra->get_uuid eq $self->{source_uuid};
    # warn "===> switching from $self->{source} to $url";
    # get a txn, change rsource and rsource_uuidto new url
}

sub get_latest_rev {
    my ($self, $ra) = @_;
    # don't care about real last-modified rev num unless in skip to mode.
    return $ra->get_latest_revnum
	unless $self->{skip_to};
    my ($rev, $headrev);
    my $offset = 2;

    until (defined $rev) {
	# there were once get_log2, but it then was refactored by the svn_ra
	# overhaul.  We have to check the version.
	# also, it's harmful to make use of the limited get_log for svn 1.2
	# vs svnserve 1.1, it retrieves all logs and leave the connection
	# in an inconsistent state.
	if ($SVN::Core::VERSION ge '1.2.0' && $self->{rsource} !~ m/^svn/) {
	    $ra->get_log ([''], -1, 1, 1, 0, 1,
			   sub { $rev = $_[1] });
	}
	else {
	    $headrev = $ra->get_latest_revnum
		unless defined $headrev;

	    $headrev -= $offset;
	    $ra->get_log ([''], -1, $headrev,
			  ($SVN::Core::VERSION ge '1.2.0') ? (0) : (),
			  0, 1,
			  sub { $rev = $_[1] unless defined $rev});
	    if ( $offset < $headrev ) {
		$offset*=2;
	    }
	    else {
		$offset = 2;
	    }
	}
    }

    die 'fatal: unable to find last-modified revision'
	unless defined $rev;
    return $rev;
}

sub run {
    my $self = shift;
    my $ra = $self->_new_ra;
    my $latestrev = $self->get_latest_rev ($ra);

    $self->lock ('sync');
    $self->load_fromrev;
    # there were code here to use find_local_rev, but it will get base that
    # is too old for use, if there are relocate happening.
    # but this might cause race condition, while we also have lock now, need
    # to take a closer look.
    $self->{headrev} = $self->{fs}->youngest_rev;
    if ($self->{skip_to} && $self->{skip_to} =~ m/^HEAD(?:-(\d+))?/) {
	$self->{skip_to} = $latestrev - ($1 || 0);
    }
    my $startrev = ($self->{skip_to} || 0);
    $startrev = $self->{fromrev}+1 if $self->{fromrev}+1 > $startrev;
    my $endrev = shift || -1;
    if ($endrev && $endrev =~ m/^HEAD(?:-(\d+))?/) {
        $endrev = $latestrev - ($1 || 0);
    }
    $endrev = $latestrev if $endrev == -1;

    print "Syncing $self->{source}".($self->_relayed ? " via $self->{rsource}\n" : "\n");

    $self->unlock ('sync'), return
	unless $endrev == -1 || $startrev <= $endrev;

    print "Retrieving log information from $startrev to $endrev\n";

    my $firsttime = 1;
    eval {
    $ra->get_log ([''], $startrev, $endrev,
		  ($SVN::Core::VERSION ge '1.2.0') ? (0) : (),
		  1, 1,
		  sub {
		      my ($paths, $rev, $author, $date, $msg, $pool) = @_;
		      # for the first time, skip_to might not hit
		      # active revision in the tree. adjust to make it so.
		      if ($firsttime) {
			  $self->{skip_to} = $rev if defined $self->{skip_to};
			  $firsttime = 0;
		      }
		      # move the anchor detection stuff to &mirror ?
		      if (defined $self->{skip_to} && $rev <= $self->{skip_to}) {
			  # XXX: get the logs for skipped changes
			  $self->{rev_incomplete} = 1;
			  $author = 'svm';
			  $msg = sprintf('SVM: skipping changes %d-%d for %s',
					 $self->{fromrev}, $rev, $self->{rsource});
		      }
		      else {
			  delete $self->{rev_incomplete};
		      }
		      $self->mirror($self->{fromrev}, $paths, $rev, $author,
				    $date, $msg, $pool);
		      $self->{fromrev} = $rev;
		  });
    };

    delete $self->{cached_ra};
    delete $self->{cached_ra_url};

    $self->unlock ('sync');

    return unless $@;
    if ($@ =~ /no item/) {
	print "Mirror source already removed.\n";
	undef $@;
    }
    else {
	die $@;
    }
}

sub DESTROY {
}

package SVN::Mirror::Ra::MirrorEditor;
our @ISA = ('SVN::Delta::Editor');
use strict;
our $debug = 0;

=begin NOTES

1. The path passed to methods in MirrorEditor is a relative path
   under $self->{anchor} (if exists) or $self->{mirror}{source_root}.

2. MirrorEditor can fetch usable information in $self->{mod_lists} if
   exists.

3. If we need to call other method in MirrorEditor, the path must be
   in the style of #1 above.

4. The diff text passed through network is patch-like.  If a directory
   is copied, there needs to be one delete_entry() call for the original
   directory, then a LOT of add_directory() and add_file() calls for
   each one of directories and files underneath the new directory.

   This behavior is easy to handle for a real file system, but hard to
   work correct for another subversion repository.  A lot of code below,
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

    if ( ($self->_in_modified_list($path) || '') eq 'R') {
	return $self->add_directory($path, $pb, undef, -1, $pool);
    }

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
    my $action = $self->_in_modified_list ($path);
    if ($self->_under_latest_copypath ($path)
	&& $self->_is_under_true_copy ($path)
	&& !$action) {
	return undef;
    }
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
	unless $_[0] =~ /^svn:(?:entry|wc):/;
}

sub change_file_prop {
    my $self = shift;
    print "MirrorEditor::change_file_prop($_[1], $_[2])\n" if $debug;
    # filter wc specified stuff
    return unless $_[0];
    return $self->SUPER::change_file_prop (@_)
	unless $_[1] =~ /^svn:(?:entry|wc):/;
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
            $path =~ s|\Q$self->{mirror}{rsource_root}\E||;
        } else {
            $path = "$self->{mirror}{rsource_path}/$path";
        }
        $path =~ s|^\Q$self->{mirror}{rsource_path}\E|$self->{mirror}{target_path}|;
        return $path;
    }

}

sub _remove_entries_in_path {
    my ($self, $path, $pb, $pool) = @_;

    foreach ( sort grep $self->{mod_lists}{$_}{action} eq 'D',
              keys %{$self->{mod_lists}} ) {
        next unless m{^\Q$path\E/([^/]+)$};
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
        $self->{mirror}{skip_to} >= $self->{mirror}{working}) {
        # no-op.
    } elsif ($self->_under_latest_copypath ($path)
            && $self->_is_under_false_copy ($path)) {
        # no-op. add_directory().
        $self->_enter_new_copied_path ();
    } elsif ($self->_under_latest_copypath ($path)
             && $self->_is_under_true_copy ($path)
             && !$action) {
        if ($self->_contains_mod_in_path ($path)) {
            $is_copy = 1;
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
	    else {
		$self->_enter_new_copied_path ()
		    if $self->_under_latest_copypath ($path);
	    }
        }
    } else {
        # raise error.
        die "Oh no, no more exceptions!  add_directory() failed.";
    }

    $method = "open_directory" if $path eq $self->{target};

    push @{$self->{visited_paths}}, $path;
    my $tran_path = $self->_translate_rel_path ($path);

    $method = 'open_directory'
        if $tran_path eq $self->{mirror}{target_path};

    splice @_, 0, 2, $self->{mirror}{headrev}
	if $method eq "open_directory";

    my $dir_baton;
    $method = "SUPER::$method";
    $dir_baton = $self->$method($tran_path, $pb, @_);

    # Always 'touch' the directory, even for empty modifications.
    $self->change_dir_prop ( $dir_baton, 'svm' => undef, $pool );

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
        
    $self->SUPER::close_directory ($baton, @_);
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
         && $self->{mirror}{skip_to} >= $self->{mirror}{working})
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
    if ($self->_under_latest_copypath($path)) {
	my $action = $self->_in_modified_list($path) || '';
	return unless $action eq 'D' || $action eq 'R';
    }
    $self->SUPER::delete_entry ($path, $self->{mirror}{headrev},
				$pb, $pool);
}

sub close_edit {
    my ($self, $pool) = @_;
    print "MirrorEditor::close_edit()\n" if $debug;

    unless ($self->{root}) {
        # If we goes here, this must be an empty revision.  We must
        # replicate an empty revision as well.
        $self->open_root ($self->{mirror}{headrev}, $pool);
	$self->SUPER::close_directory ($self->{root}, $pool);
    }
    delete $self->{root};
    local $SIG{INT} = 'IGNORE';
    local $SIG{TERM} = 'IGNORE';

    $self->{mirror}->lock ('mirror');
    $self->SUPER::close_edit ($pool);
}

1;
