#!/usr/bin/perl
use SVN::Core;
package MirrorEditor;
@ISA = ('SVN::Delta::Editor');
use strict;

use constant VSNURL => 'svn:wc:ra_dav:version-url';

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
    $self->{root} = $self->SUPER::open_root($self->{mirror}{headrev}, $pool);
}

sub open_directory {
    my ($self,$path,$pb,undef,$pool) = @_;
    return undef unless $pb;
    return $self->{root}
	if $self->{target} && ($self->{target} eq $path ||
	    "$path/" eq substr($self->{target}, 0, length($path)+1));
    $path =~ s|^$self->{target}/|| or return undef
	if $self->{target};

    return $self->SUPER::open_directory ($path, $pb,
					 $self->{mirror}{headrev}, $pool);
}

sub open_file {
    my ($self,$path,$pb,undef,$pool) = @_;
    return undef unless $pb;
    $path =~ s|^$self->{target}/|| or return undef
	if $self->{actuacl_target};
    $self->{opening} = $path;
    return $self->SUPER::open_file ($path, $pb,
				    $self->{mirror}{headrev}, $pool);
}

sub change_dir_prop {
    my $self = shift;
    my $baton = shift;
    # filter wc specified stuff
    return unless $baton;
    return $self->SUPER::change_dir_prop ($baton, @_)
	unless $_[0] =~ /^svn:(entry|wc):/;
    $self->{NEWVSN} = $_[1]
	if $baton == $self->{root} && $_[0] eq VSNURL;
}

sub change_file_prop {
    my $self = shift;
    # filter wc specified stuff
    return unless $_[0];
    return $self->SUPER::change_file_prop (@_)
	unless $_[1] =~ /^svn:(entry|wc):/;
}

sub add_directory {
    my $self = shift;
    my $path = shift;
    my $pb = shift;
    my ($cp_path,$cp_rev,$pool) = @_;
    return undef unless $pb;
    return $self->{root} = $pb if $self->{target} &&
	$self->{target} eq $path;
    $path =~ s|^$self->{target}/|| or return undef
	if $self->{target};

=for comment

    my ($rev, $frompath) = SVN::Fs::copied_from($self->{root}, $path, $pool);
    if ($frompath) {
	push @{$self->{copied}}, $path;
	warn "add dir ($rev, $frompath) -> $_[0]";
	return $self->SUPER::add_directory($path, $baton, $frompath, $rev, $pool);
    }
    return undef if $self->_ignore($path);

=cut

    $self->SUPER::add_directory($path, $pb, @_);
}

sub _ignore {
    my ($self, $path) = @_;
    for my $c (@{$self->{copied}}) {
	return 1 if index ($path, $c, 0) == 0;
    }
    return 0;
}

sub apply_textdelta {
    my $self = shift;
    return undef unless $_[0];

    $self->SUPER::apply_textdelta (@_);
}

sub close_directory {
    my $self = shift;
    my $baton = shift;
    return unless $baton;
    $self->{mirror}{VSN} = $self->{NEWVSN}
	if $baton == $self->{root} && $self->{NEWVSN};
    $self->SUPER::close_directory ($baton);
}

sub close_file {
    my $self = shift;
    return unless $_[0];
    $self->SUPER::close_file(@_);
}

sub add_file {
    my $self = shift;
    my $path = shift;
    my $pb = shift;
    return undef unless $pb;
    $path =~ s|^$self->{target}/|| or return undef
	if $self->{target};

    return undef if $self->_ignore($path);

    $self->SUPER::add_file($path, $pb, @_);
}

sub delete_entry {
    my ($self, $path, $rev, $pb, $pool) = @_;
    return unless $pb;
    $self->SUPER::delete_entry ($path, $rev, $pb, $pool);
}

sub close_edit {
    my ($self) = @_;
    $self->SUPER::close_directory ($self->{root});
    $self->SUPER::close_edit (@_);
}

package MyCallbacks;

use SVN::Ra;
our @ISA = ('SVN::Ra::Callbacks');

sub get_wc_prop {
    my ($self, $relpath, $name, $pool) = @_;
    return undef unless $self->{editor}{opening};
    return undef unless $name eq 'svn:wc:ra_dav:version-url';
    return join('/', $self->{mirror}{VSN}, $relpath)
	if $self->{mirror}{VSN} &&
	    $self->{editor}{opening} eq $relpath; # skip add_file

    return undef;
}

package SVN::Mirror;
our $VERSION = '0.25';
use SVN::Core;
use SVN::Repos;
use SVN::Fs;
use SVN::Delta;
use SVN::Ra;
use SVN::Client ();
use strict;

=head1 NAME

SVN::Mirror - Mirror Remote Subversion Repository to local

=head1 SYNOPSIS

my $m = SVN::Mirror->new (source => $url,
			  target => '/path/to/repository',
			  target_path => '/mirror/project1'
			  target_create => 1,
			  skip_to => 100
			 );

$m->init

$m->run

=head1 DESCRIPTION


=cut

use File::Spec;
use URI::Escape;


sub new {
    my $class = shift;
    my $self = bless {}, $class;
    %$self = @_;

    die "no repository specified" unless $self->{target} || $self->{repos};

    $self->{pool} ||= SVN::Pool->new_default (undef);
    if ($self->{target_create} && !-e $self->{target}) {
	$self->{repos} = SVN::Repos::create($self->{target},
					    undef, undef, undef, undef);
    }
    elsif ($self->{repos}) {
	$self->{target} = $self->{repos}->path;
    }

    $self->{repos} ||= SVN::Repos::open ($self->{target});

    $self->{fs} = $self->{repos}->fs;

    return $self;
}

sub is_mirrored {
    my ($repos, $path) = @_;

    my $m = SVN::Mirror->new(target_path => $path,
			     repos => $repos,
			     pool => SVN::Pool->new,
			     get_source => 1) or die $@;
    eval { $m->init };
    return if $@;
    return $m;
}

sub find_local_rev {
    my ($self, $rrev) = @_;
    my $pool = SVN::Pool->new_default ($self->{pool});

    my $fs = $self->{repos}->fs;

    my $hist = $fs->revision_root ($fs->youngest_rev)->
	node_history ($self->{target_path});

    while ($hist = $hist->prev (0)) {
	my $rev = ($hist->location)[1];
	return $rev if $rrev ==
	    $self->{fs}->revision_prop ($rev, "svm:headrev:$self->{source}");
    }

    die "unable to resolve remote revision $rrev to local revision";
}

sub init {
    my $self = shift;
    my $pool = SVN::Pool->new_default ($self->{pool});
    my $headrev = $self->{headrev} = $self->{fs}->youngest_rev;
    $self->{root} = $self->{fs}->revision_root ($headrev);

    my $txn = $self->{fs}->begin_txn ($headrev);
    my $txnroot = $txn->root;
    my $new = $self->mkpdir ($txnroot, $self->{target_path});

    $self->{config} = SVN::Core::config_get_config(undef, $self->{pool});

    unless ($new) {
	my $prop = $txnroot->node_proplist ($self->{target_path});
	if (my $remote = $prop->{'svm:source'}) {
	    my ($root, $path) = split ('!', $remote);
	    warn "old style svm:source"
		unless defined $path;
	    $remote = join('', $root, $path);
	    die "different source"
		if !$self->{get_source} && $remote ne $self->{source};
	    $self->{source} = $remote if $self->{get_source};
	    $self->{source_root} = $root
		if defined $path;

	    # check revprop of target_path's latest rev for headrev
	    my $changed = $self->{root}->node_created_rev ($self->{target_path});

	    my $prop = $self->{fs}->revision_proplist ($changed);
	    die "no headrev for $self->{source} at rev $changed"
		unless exists $prop->{"svm:headrev:$self->{source}"};
	    $self->{fromrev} = $prop->{"svm:headrev:$self->{source}"};
	    $self->{VSN} = $prop->{"svm:vsnroot:$self->{source}"};
	    $txn->abort ();
	    return;
	}
    }

    die "svm not configured on $self->{target_path}"
	if $self->{get_source};

    my $ra = SVN::Ra->new(url => $self->{source},
			  auth => $self->{auth},
			  pool => $self->{pool},
			  config => $self->{config},
			  callback => 'MyCallbacks');

    my $uuid = $ra->get_uuid ();
    my $source_root = $ra->get_repos_root ();

    $txnroot->change_node_prop ($self->{target_path},
				'svm:uuid', "$uuid");

    my $url = $self->{source};
    $url =~ s/^$source_root//;

    $txnroot->change_node_prop ($self->{target_path}, 'svm:source',
				join('!', $source_root, $url));

    my (undef, $rev) = $txn->commit ();

    print "Committed revision $rev.\n";

    $self->{fromrev} = 0;
    $self->{fs}->change_rev_prop ($rev, "svm:headrev:$self->{source}", 0);

    $self->{fs}->change_rev_prop ($rev, "svn:author", 'svm');

    $self->{fs}->change_rev_prop
	($rev, "svn:log", "SVM: initialziing mirror for $self->{target}");
    $self->{headrev} = $self->{fs}->youngest_rev;
}

sub mkpdir {
    my ($self, $root, $dir) = @_;
    my @dirs = File::Spec->splitdir($self->{target_path});
    my $path = '';
    my $new;

    while (@dirs) {
	$path = File::Spec->join($path, shift @dirs);
	my $kind = $self->{root}->check_path ($path);
	if ($kind == $SVN::Core::node_none) {
	    $root->make_dir ($path, $self->{pool});
	    $new = 1;
	}
	elsif ($kind != $SVN::Core::node_dir) {
	    die "something is in the way of mirror root($path)";
	}
    }
    return $new;
}

sub committed {
    my ($self, $date, $sourcerev, $rev, undef, undef, $pool) = @_;
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

    my $editor = MirrorEditor->new
	($self->{repos}->get_commit_editor
	 ('', $self->{target_path}, $author, $msg,
	  sub { $self->committed($date, $rev, @_) }));

    $editor->{mirror} = $self;

    $ra = $self->{cached_ra}
	if exists $self->{cached_ra_url} &&
	    $self->{cached_ra_url} eq $self->{source};

    $ra ||= SVN::Ra->new(url => $self->{source},
			 auth => $self->{auth},
			 pool => SVN::Pool->new,
			 config => $self->{config},
			 callback => 'MyCallbacks');
    @{$self}{qw/cached_ra cached_ra_url/} = ($ra, $self->{source});

    if ($fromrev == 0 && $self->{source} ne $self->{source_root}) {
	(undef, $editor->{anchor}, $editor->{target})
	    = File::Spec->splitpath($editor->{anchor} || $self->{source});
	chop $editor->{anchor};
	$ra = SVN::Ra->new(url => $editor->{anchor},
			   pool => SVN::Pool->new,
			   auth => $self->{auth},
			   config => $self->{config},
			   callback => 'MyCallbacks');

	@{$self}{qw/cached_ra cached_ra_url/} = ($ra, $editor->{anchor});
    }
    $ra->{callback}{mirror} = $self;
    $ra->{callback}{editor} = $editor;

    my $reporter =
	$ra->do_update ($rev, $editor->{target}, 1, $editor);

=for comment TODO - discover copy history in log output

    my @visited;
    while (my ($path, $info) = each %$paths) {
	if ($info->copyfrom_path) {
	    $editor->{history}{$path} = $info->copyfrom_rev;
	    warn "someone has history";
	}
    }

=cut

#    my $start = $self->{skip_to} ? $fromrev : $rev-1;
    my $start = $fromrev || ($self->{skip_to} ? $fromrev : $rev-1);
    $reporter->set_path ('', $start, 0);
    $reporter->finish_report ();
}

sub get_merge_back_editor {
    my ($self, $msg, $committed) = @_;
    # get ra commit editor for $self->{source}
    my $ra = SVN::Ra->new(url => $self->{source},
			  auth => $self->{auth},
			  pool => $self->{pool},
			  config => $self->{config},
			  callback => 'MyCallbacks');
    my $youngest_rev = $ra->get_latest_revnum;

    return ($youngest_rev,
	    SVN::Delta::Editor->new ($ra->get_commit_editor ($msg, $committed)));
}

sub mergeback {
    my ($self, $fromrev, $path, $rev) = @_;

    # verify $path is copied from $self->{target_path}

    # concat batch merge?
    my $msg = $self->{fs}->revision_prop ($rev, 'svn:log');
    $msg .= "\n\nmerged from rev $rev of repository ".$self->{fs}->get_uuid;

    my $editor = $self->get_merge_back_editor ($msg,
					       sub {warn "committed via RA"});

    # dir_delta ($path, $fromrev, $rev) for commit_editor
    SVN::Repos::dir_delta($self->{fs}->revision_root ($fromrev), $path, undef,
			  $self->{fs}->revision_root ($rev), $path,
			  $editor, undef,
			  1, 1, 0, 1
			 );
}

sub run {
    my $self = shift;
    my $startrev = ($self->{skip_to} || 1)-1;
    $startrev = $self->{fromrev}+1 if $self->{fromrev}+1 > $startrev;
    my $endrev = shift || -1;
    my $ra = SVN::Ra->new(url => $self->{source}, pool => $self->{pool},
 			  auth => $self->{auth});

    $endrev = $ra->get_latest_revnum () if $endrev == -1;

    print "Syncing $self->{source}\n";

    return unless $endrev == -1 || $startrev <= $endrev;

    print "Retrieving log information for $startrev to $endrev\n";

    $ra->get_log ([''], $startrev, $endrev, 0, 1,
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

=for comment

sub list {
    my $self = shift;
    my $mirrors;

    for (1..$self->{repos}->fs->youngest_rev) {
	my $prop = $self->{fs}->revision_proplist ($changed, $changed);
	for (my ($k, $v) = each %$prop) {
#	    if ($k =~ ) {
#	    }
	    $mirrors->{}
	}
    }
    $self->{repos}->get_log
}

=cut

=head1 AUTHORS

Chia-liang Kao E<lt>clkao@clkao.orgE<gt>

=head1 COPYRIGHT

Copyright 2003 by Chia-liang Kao E<lt>clkao@clkao.orgE<gt>.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See L<http://www.perl.com/perl/misc/Artistic.html>

=cut

1;
