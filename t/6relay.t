#!/usr/bin/perl
use Test::More;
use SVN::Mirror;
use File::Path;
use File::Spec;
use strict;

plan skip_all => "relay doesn't work with svn < 1.1.0"
    unless $SVN::Core::VERSION ge '1.1.0';

plan skip_all => "can't find svnadmin"
    unless -x '/usr/local/bin/svnadmin' || -x '/usr/bin/svnadmin';

plan tests => 9;
my $repospath = "t/repos";

rmtree ([$repospath]) if -d $repospath;
$ENV{SVNFSTYPE} ||= (($SVN::Core::VERSION =~ /^1\.0/) ? 'bdb' : 'fsfs');

my $repos = SVN::Repos::create($repospath, undef, undef, undef,
			       {'fs-type' => $ENV{SVNFSTYPE}})
    or die "failed to create repository at $repospath";

my $abs_path = File::Spec->rel2abs( $repospath ) ;
`svn mkdir -m 'init' file://$abs_path/source`;
`svnadmin load --parent-dir source $repospath < t/test_repo.dump`;

my $rrepospath = 't/repos.relayed';
rmtree ([$rrepospath]) if -d $rrepospath;
my $rrepos = SVN::Repos::create($rrepospath, undef, undef, undef,
				{'fs-type' => $ENV{SVNFSTYPE} || 'bdb'})
    or die "failed to create repository at $rrepospath";
my $rabs_path = File::Spec->rel2abs( $rrepospath ) ;

for (1..50) {
    `svn mkdir -m 'waste rev' file://$rabs_path/waste`;
    `svn rm -m 'waste rev' file://$rabs_path/waste`;
}

my $m = SVN::Mirror->new(target_path => '/fullcopy', repos => $rrepos,
			 source => "file://$abs_path/source");
$m->init;
is ($m->{source}, "file://$abs_path/source");
is ($m->{rsource}, "file://$abs_path/source");
$m->run;


$m = SVN::Mirror->new(target_path => '/newcopy', repos => $rrepos,
		      source => "file://$rabs_path/fullcopy");
$m->init;
is ($m->{source}, "file://$abs_path/source");
is ($m->{rsource}, "file://$rabs_path/fullcopy");

$m = SVN::Mirror->new (target_path => '/newcopy', repos => $rrepos,
		       get_source => 1);
$m->init;
is ($m->{source}, "file://$abs_path/source");
is ($m->{rsource}, "file://$rabs_path/fullcopy");
$m->run;
#print `svn log -v file://$rabs_path`;
$m->switch ("file://$abs_path/source");

$m = SVN::Mirror->new(target_path => '/newcopy-svnperl', repos => $rrepos,
		      source => "file://$rabs_path/fullcopy/svnperl");
$m->init;
is ($m->{source}, "file://$abs_path/source/svnperl");
is ($m->{rsource}, "file://$rabs_path/fullcopy/svnperl");
$m->run;

$m = SVN::Mirror->new(target_path => '/newcopy-root', repos => $rrepos,
		      source => "file://$rabs_path/");
eval { $m->init };

ok ($@ =~ m/outside mirror anchor/);
