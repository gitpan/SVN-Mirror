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
	if $self->{actual_target} && ($self->{actual_target} eq $path ||
	    "$path/" eq substr($self->{actual_target}, 0, length($path)+1));
    $path =~ s|^$self->{actual_target}/|| or return undef
	if $self->{actual_target};

    return $self->SUPER::open_directory ($path, $pb,
					 $self->{mirror}{headrev}, $pool);
}

sub open_file {
    my ($self,$path,$pb,undef,$pool) = @_;
    return undef unless $pb;
    $path =~ s|^$self->{actual_target}/|| or return undef
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
    return $self->{root} = $pb if $self->{actual_target} &&
	$self->{actual_target} eq $path;
    $path =~ s|^$self->{actual_target}/|| or return undef
	if $self->{actual_target};

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
    $path =~ s|^$self->{actual_target}/|| or return undef
	if $self->{actual_target};

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
our $VERSION = '0.21';
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

    die "no repository specified" unless $self->{target};

    $self->{pool} ||= SVN::Pool->new_default (undef);
    if ($self->{target_create} && !-e $self->{target}) {
	$self->{repos} = SVN::Repos::create($self->{target},
					    undef, undef, undef, undef);
    }
    else {
	$self->{repos} = SVN::Repos::open ($self->{target});
    }
    $self->{fs} = $self->{repos}->fs;

    return $self;
}

sub init {
    my $self = shift;
    $self->{pool}->default;
    my $headrev = $self->{headrev} = $self->{fs}->youngest_rev;
    $self->{root} = $self->{fs}->revision_root ($headrev);

    my $txn = SVN::Fs::begin_txn($self->{fs}, $headrev);
    my $txnroot = $txn->root;
    my $new = $self->mkpdir ($txnroot, $self->{target_path});

    $self->{config} = SVN::Core::config_get_config(undef);

    unless ($new) {
	my $prop = SVN::Fs::node_proplist($txnroot, $self->{target_path});
	if (my $remote = $prop->{'svm:source'}) {
	    die "different source"
		if !$self->{get_source} && $remote ne $self->{source};
	    $self->{source} = $remote if $self->{get_source};
	    # check revprop of target_path's latest rev for headrev
	    # $self->{fromrev} = ...
	    my $changed = SVN::Fs::node_created_rev ($self->{root},
						     $self->{target_path});

	    my $prop = $self->{fs}->revision_proplist ($changed);
	    die "no headrev"
		unless exists $prop->{"svm:headrev:$self->{source}"};
	    $self->{fromrev} = $prop->{"svm:headrev:$self->{source}"};
	    $self->{VSN} = $prop->{"svm:vsnroot:$self->{source}"};
	    SVN::Fs::abort_txn ($txn);
	    return;
	}
    }

    die "svm not configured on $self->{target_path}"
	if $self->{get_source};

    SVN::Fs::change_node_prop ($txnroot, $self->{target_path},
			       'svm:source', "$self->{source}");

    my (undef, $rev) = SVN::Fs::commit_txn($txn);

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
	my $kind = SVN::Fs::check_path($self->{root}, $path);
	if ($kind == $SVN::Core::node_none) {
	    SVN::Fs::make_dir ($root, $path, $self->{pool});
	    $new = 1;
	}
	elsif ($kind != $SVN::Core::node_dir) {
	    die "something is in the way of mirror root($path)";
	}
    }
    return $new;
}

sub committed {
    my ($self, $date, $sourcerev, $pool, $rev) = @_;
    $pool->default;
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

    my $editor = new MirrorEditor SVN::Repos::get_commit_editor
	($self->{repos}, '', $self->{target_path}, $author, $msg,
	 sub { $self->committed($date, $rev, $pool, @_) });

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
    # iterate over upper level to get the real anchor
    if ($fromrev == 0) {
	while ($ra->check_path("", $self->{fromrev}) == $SVN::Core::node_none) {
	    (undef, $editor->{anchor}, $editor->{target})
		= File::Spec->splitpath($editor->{anchor} || $self->{source});
	    chop $editor->{anchor};
	    $ra = SVN::Ra->new(url => $editor->{anchor},
			       pool => SVN::Pool->new,
			       auth => $self->{auth},
			       config => $self->{config},
			       callback => 'MyCallbacks');

	    @{$self}{qw/cached_ra cached_ra_url/} = ($ra, $editor->{anchor});

#	    warn "new anchor is $editor->{anchor}, new target is $editor->{target}";
	    $editor->{actual_target} = $editor->{actual_target} ?
		"$editor->{target}/$editor->{actual_target}" : $editor->{target};
	}
    }
    $ra->{callback}{mirror} = $self;
    $ra->{callback}{editor} = $editor;

    my $reporter =
	$ra->do_diff ($rev, undef, 1, 1,
		      $editor->{anchor} || $self->{source}, $editor) ;

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
    $editor->{target} ||= '';
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

    return SVN::Delta::Editor->new ($ra->get_commit_editor ($msg, $committed));
}

sub mergeback {
    my ($self, $fromrev, $path, $rev) = @_;

    # verify $path is copied from $self->{target_path}

    # concat batch merge?
    my $msg = $self->{fs}->revision_prop ($rev, 'svn:log');
    $msg .= "\n\nmerged from rev $rev of repository ".
	SVN::Fs::get_uuid($self->{fs});

    my $editor = $self->get_merge_back_editor ($msg,
					       sub {warn "committed via RA"});

    # dir_delta ($path, $fromrev, $rev) for commit_editor
    SVN::Repos::dir_delta($self->{fs}->revision_root ($fromrev), $path, undef,
			  $self->{fs}->revision_root ($rev), $path,
			  $editor,
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
