#!/usr/bin/perl
package SVN::Mirror;
our $VERSION = '0.35';
use SVN::Core;
use SVN::Repos;
use SVN::Fs;
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
use SVN::Simple::Edit;

use SVN::Mirror::Ra;
eval 'use SVN::Mirror::VCP';

sub _schema_class {
    my ($url) = @_;
    return 'SVN::Mirror::Ra' if $url =~ m/^(https?|file|svn(\+.*?)?):/;
    return 'SVN::Mirror::VCP' if $url =~ m/^(p4|cvs):/;

    die "schema for $url not handled";
}

sub new {
    my $class = shift;
    my $self = {};
    %$self = @_;

    return bless $self, $class unless $class eq __PACKAGE__;

    # XXX: legacy argument
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

    $self->{source} ||= $root->node_prop ($self->{target_path}, 'svm:source')
	or die "no source found on $self->{target_path}";

    return _schema_class ($self->{source})->new (%$self);
}

sub has_local {
    my ($repos, $spec) = @_;
    my $fs = $repos->fs;
    my $root = $fs->revision_root ($fs->youngest_rev);
    my $prop = $root->node_proplist ('/');
    my ($specanchor) =
	map { substr ("svm:mirror:$spec/", 0, length($_)+1) eq "$_/" ? $_ : () }
	    keys %$prop;
    return unless $specanchor;
    my $path = $prop->{$specanchor};
    my $mpath = "svm:mirror:$spec";
    $mpath =~ s/^\Q$specanchor\E//;
    $path =~ s/^\Q$mpath\E//;
    my $m = SVN::Mirror->new (target_path => $path,
			     repos => $repos,
			     pool => SVN::Pool->new,
			     get_source => 1);
    eval { $m->init () };
    undef $@, return if $@;
    return wantarray ? ($m, $mpath) : $m;
}

sub list_mirror {
    my ($repos) = @_;
    my $fs = $repos->fs;
    my $root = $fs->revision_root ($fs->youngest_rev);

    my $prop = $root->node_proplist ('/');

    return map {$prop->{$_}} grep {m/^svm:mirror:/} keys %$prop;
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

sub find_local_rev {
    my ($self, $rrev) = @_;
    return if $rrev > ($self->{working} || $self->{fromrev});
    my $pool = SVN::Pool->new_default ($self->{pool});

    my $fs = $self->{repos}->fs;

    # We cannot try to get node_history() on $self->{target_path}.  If
    # a copy is from an empty revision, we'll never get its local
    # revision.  An empty revision could only be found based on root
    # path.  For real example, see r2770 to r2769 in
    # http://svn.collab.net/repos/svn.
    my $hist = $fs->revision_root ($fs->youngest_rev)->
	node_history ($self->{target_path});

    while ($hist = $hist->prev (0)) {
	my $rev = ($hist->location)[1];
        my $lrev =
            $self->{fs}->revision_prop ($rev, "svm:headrev:$self->{source}");
        next unless defined $lrev;
	return $rev if $rrev >= $lrev;
    }

    return;
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
	$txnroot->change_node_prop ('/', join (':', 'svm:mirror', $self->{source_uuid}, 
					       ($self->{source_path} || '/')),
				    $self->{target_path});
	$txnroot->change_node_prop ($self->{target_path}, 'svm:source', $source);
	my (undef, $rev) = $txn->commit ();
	print "Committed revision $rev.\n";

	$self->{fs}->change_rev_prop ($rev, "svm:headrev:$self->{source}", $self->{fromrev})
	    if defined $self->{fromrev};
	$self->{fs}->change_rev_prop ($rev, "svn:author", 'svm');
	$self->{fs}->change_rev_prop
	    ($rev, "svn:log", "SVM: initialziing mirror for $self->{target}");
    }
    else {
	$self->load_state ($txn);
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

=head1 AUTHORS

Chia-liang Kao E<lt>clkao@clkao.orgE<gt>

=head1 COPYRIGHT

Copyright 2003 by Chia-liang Kao E<lt>clkao@clkao.orgE<gt>.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See L<http://www.perl.com/perl/misc/Artistic.html>

=cut

1;
