#!/usr/bin/perl
package SVN::Mirror;
our $VERSION = '0.41';
use SVN::Core;
use SVN::Repos;
use SVN::Fs;
use strict;

=head1 NAME

SVN::Mirror - Mirror remote repository to local Subversion repository

=head1 SYNOPSIS

 my $m = SVN::Mirror->new (source => $url,
			   target => '/path/to/repository',
			   target_path => '/mirror/project1'
			   target_create => 1,
			   skip_to => 100
			  );
 $m->init;
 $m->run;

=head1 DESCRIPTION

SVN::Mirror allows you to mirror remote repository to your local
subversion repository. Supported types of remote repository are:

=over

=item Subversion

with the L<SVN::Mirror::Ra> backend.

=item CVS, Perforce

with the L<SVN::Mirror::VCP> backend through the L<VCP> framework.

=back

=cut

use File::Spec;
use URI::Escape;
use SVN::Simple::Edit;

use SVN::Mirror::Ra;
my $has_vcp;

eval 'use SVN::Mirror::VCP; 1' and $has_vcp++;

sub _schema_class {
    my ($url) = @_;
    die "no source specificed" unless $url;
    return 'SVN::Mirror::Ra' if $url =~ m/^(https?|file|svn(\+.*?)?):/;
    return 'SVN::Mirror::VCP' if $has_vcp && $url =~ m/^(p4|cvs):/;

    die "schema for $url not handled";
}

sub new {
    my $class = shift;
    my $self = {};
    %$self = @_;

    return bless $self, $class unless $class eq __PACKAGE__;

    # XXX: legacy argument to be removed.
    $self->{repospath} ||= $self->{target};
    $self->{repos_create} ||= $self->{target_create};
    $self->{target} ||= $self->{repospath};
    $self->{target_create} ||= $self->{repos_create};

    die "no repository specified" unless $self->{repospath} || $self->{repos};

    die "no source specified" unless $self->{source} || $self->{get_source};

    $self->{pool} ||= SVN::Pool->new (undef);
    if ($self->{repos_create} && !-e $self->{repospath}) {
	$self->{repos} = SVN::Repos::create($self->{repospath},
					    undef, undef, undef, undef, $self->{pool});
    }
    elsif ($self->{repos}) {
	$self->{repospath} = $self->{repos}->path;
    }

    $self->{repos} ||= SVN::Repos::open ($self->{repospath}, $self->{pool});
    my $fs = $self->{fs} = $self->{repos}->fs;

    my $root = $fs->revision_root ($fs->youngest_rev);
    $self->{target_path} =~ s{/+$}{}g;
    $self->{target_path} = '/'.$self->{target_path}
	unless substr ($self->{target_path}, 0, 1) eq '/';

    if ($root->check_path ($self->{target_path}) != $SVN::Node::none) {
	$self->{rsource} = $root->node_prop ($self->{target_path}, 'svm:rsource');
	$self->{source} ||= $root->node_prop ($self->{target_path}, 'svm:source')
	    or die "no source found on $self->{target_path}";
    }

    return _schema_class ($self->{rsource} || $self->{source})->new (%$self);
}

sub source {

}

sub has_local {
    my ($repos, $spec) = @_;
    my $fs = $repos->fs;
    my $root = $fs->revision_root ($fs->youngest_rev);
    my %mirrored = map {join(':', $root->node_prop ($_, 'svm:uuid'),
			 $root->node_prop ($_, 'svm:source') =~ m/\!(.*)$/)
		    => $_} list_mirror ($repos);
    my ($specanchor) =
	map { (substr ($_, -1, 1) eq '/' ?
	       substr ($spec, 0, length ($_)) eq $_
	       : substr ("$spec/", 0, length($_)+1) eq "$_/")
		  ? $_ : () } keys %mirrored;
    return unless $specanchor;
    my $path = $mirrored{$specanchor};
    $spec =~ s/^\Q$specanchor\E//;
    $path =~ s/^\Q$spec\E//;
    $spec = "/$spec" if $spec && substr ($spec, 0, 1) ne '/';
    my $m = SVN::Mirror->new (target_path => $path,
			     repos => $repos,
			     pool => SVN::Pool->new,
			     get_source => 1);
    eval { $m->init () };
    undef $@, return if $@;
    return wantarray ? ($m, $spec) : $m;
}

sub list_mirror {
    my ($repos) = @_;
    my $fs = $repos->fs;
    my $root = $fs->revision_root ($fs->youngest_rev);
    die "please upgrade the mirror state\n"
	if grep {m/^svm:mirror:/} keys %{$root->node_proplist ('/')};

    my $prop = $root->node_prop ('/', 'svm:mirror') or return;
    return $prop =~ m/^(.*)$/mg;
}

sub is_mirrored {
    my ($repos, $path) = @_;
    my ($mpath) = map { substr ("$path/", 0, length($_)+1) eq "$_/" ? $_ : () } list_mirror ($repos);
    return unless $mpath;
    $path =~ s/^\Q$mpath\E//;

    my $m = SVN::Mirror->new (target_path => $mpath,
			      repos => $repos,
			      pool => SVN::Pool->new,
			      get_source => 1) or die $@;
    eval { $m->init };
    undef $@, return if $@;
    return wantarray ? ($m, $path) : $m;
}

my %CACHE;

sub find_local_rev {
    my ($self, $rrev, $uuid) = @_;
    return if $rrev > ($self->{working} || $self->{fromrev});
    my $pool = SVN::Pool->new_default ($self->{pool});

    my $fs = $self->{repos}->fs;
    $uuid ||= $self->{source_uuid};
    # XXX: make better use of the cache
    my $cache = $CACHE{$uuid} ||= {};
    return $cache->{$rrev} if exists $cache->{$rrev};
    # We cannot try to get node_history() on $self->{target_path}.  If
    # a copy is from an empty revision, we'll never get its local
    # revision.  An empty revision could only be found based on root
    # path.  For real example, see r2770 to r2769 in
    # http://svn.collab.net/repos/svn.
    my $hist = $fs->revision_root ($fs->youngest_rev)->
	node_history ($self->{target_path});

    while ($hist = $hist->prev (0)) {
	my $rev = ($hist->location)[1];
	my $prop = $self->{fs}->revision_prop ($rev, 'svm:headrev');
	my ($lrev) = $prop =~ m/^\Q$uuid\E:(\d+)$/m;
        next unless defined $lrev;
	$cache->{$lrev} = $rev;
	# XXX: make use of svm:incomplete prop to see if this is safe to use
	return $rev if $rrev >= $lrev;
    }

    return;
}

=head2 find_remote_rev



=cut

sub find_remote_rev {
    my ($self, $rev, $repos) = @_;
    $repos ||= $self->{repos};
    my $fs = $repos->fs;
    my $prop = $fs->revision_prop ($rev, 'svm:headrev') or return;
    my %rev = map {split (':', $_, 2)} $prop =~ m/^.*$/mg;
    return %rev if wantarray;
    return ref($self) ? $rev{$self->{source_uuid}} || $rev{$self->{rsource_uuid}} :
	(values %rev)[0];
}

sub delete {
    my $self = shift;
    my $fs = $self->{repos}->fs;
    my $newprop = join ('', map {"$_\n"} grep { $_ ne $self->{target_path}}
			list_mirror ($self->{repos}));
    my $txn = $fs->begin_txn ($fs->youngest_rev);
    my $txnroot = $txn->root;
    $txn->change_prop ("svn:author", 'svm');
    $txn->change_prop ("svn:log", "SVM: discard mirror for $self->{target_path}");
    $txnroot->change_node_prop ('/', 'svm:mirror', $newprop);
    my (undef, $rev) = $txn->commit ();
    print "Committed revision $rev.\n";
}

# prepare source
sub pre_init {}

sub init {
    my $self = shift;
    my $pool = SVN::Pool->new_default ($self->{pool});
    my $headrev = $self->{headrev} = $self->{fs}->youngest_rev;
    $self->{root} = $self->{fs}->revision_root ($headrev);

    my $txn = $self->{fs}->begin_txn ($headrev);
    my $txnroot = $txn->root;
    my $new = $self->mkpdir ($txnroot, $self->{target_path});

    $self->{config} = SVN::Core::config_get_config(undef, $self->{pool});

    $self->pre_init ($new);

    if ($new) {
	my $source = $self->init_state ($txn);
	my $mirrors = $txnroot->node_prop ('/', 'svm:mirror') || '';
	$txnroot->change_node_prop ('/', 'svm:mirror', "$mirrors$self->{target_path}\n");
	$txnroot->change_node_prop ($self->{target_path}, 'svm:source', $source);
	$txnroot->change_node_prop ($self->{target_path}, 'svm:uuid', $self->{source_uuid});

	my (undef, $rev) = $txn->commit ();
	print "Committed revision $rev.\n";

	$self->{fs}->change_rev_prop ($rev, "svn:author", 'svm');
	$self->{fs}->change_rev_prop
	    ($rev, "svn:log", "SVM: initialziing mirror for $self->{target_path}");
    }
    else {
	$self->load_state ();
	$txn->abort;
    }
}

sub mergeback {
    my ($self, $fromrev, $path, $rev) = @_;

    # verify $path is copied from $self->{target_path}

    # concat batch merge?
    my $msg = $self->{fs}->revision_prop ($rev, 'svn:log');
    $msg .= "\n\nmerged from rev $rev of repository ".$self->{fs}->get_uuid;

    my $editor = $self->get_merge_back_editor ('', $msg,
					       sub {warn "committed via RA"});

    # dir_delta ($path, $fromrev, $rev) for commit_editor
    SVN::Repos::dir_delta($self->{fs}->revision_root ($fromrev), $path,
			  $SVN::Core::VERSION ge '0.36.0' ? '' : undef,
			  $self->{fs}->revision_root ($rev), $path,
			  $editor, undef,
			  1, 1, 0, 1
			 );
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

sub upgrade {
    my ($repos) = @_;
    my $fs = $repos->fs;
    my $yrev = $fs->youngest_rev;

    # pre 0.40:
    # svm:mirror:<uuid>:<path> in node_prop of /
    # svm:headrev:<url>

    my $txn = $fs->begin_txn ($yrev);
    my $root = $txn->root;
    my $prop = $root->node_proplist ('/');
    my @mirrors;
    for (grep {m/^svm:mirror:/} keys %$prop) {
	$root->change_node_prop ('/', $_, undef);
	push @mirrors, $prop->{$_};
    }

    unless (@mirrors) {
	print "nothing to upgrade\n";
	$txn->abort;
	return;
    }

    $root->change_node_prop ('/', 'svm:mirror', join ('', map {"$_\n"} @mirrors));

    my $spool = SVN::Pool->new_default;
    for (@mirrors) {
	print "Upgrading $_.\n";
	my $source = join ('', split ('!', $root->node_prop ($_, 'svm:source')));
	my $uuid = $root->node_prop ($_, 'svm:uuid');
	my $hist = $fs->revision_root ($yrev)->node_history ($_);
	my $ipool = SVN::Pool->new_default_sub;
	while ($hist = $hist->prev (0)) {
	    my (undef, $rev) = $hist->location;
	    my $rrev = $fs->revision_prop ($rev, "svm:headrev:$source");
	    die "no headrev" unless defined $rrev;
	    $fs->change_rev_prop ($rev, "svm:headrev:$source", undef);
	    $fs->change_rev_prop ($rev, "svm:headrev", "$uuid:$rrev\n");
	    $ipool->clear;
	}
    }

    my (undef, $rev) = $txn->commit;
    $fs->change_rev_prop ($rev, "svn:author", 'svm');
    $fs->change_rev_prop ($rev, "svn:log", 'SVM: upgrading svm mirror state.');
}

=head1 AUTHORS

Chia-liang Kao E<lt>clkao@clkao.orgE<gt>

=head1 COPYRIGHT

Copyright 2003-2004 by Chia-liang Kao E<lt>clkao@clkao.orgE<gt>.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See L<http://www.perl.com/perl/misc/Artistic.html>

=cut

1;
