#!/usr/bin/perl -w
use strict;
use Test::More;
use SVN::Mirror;
use File::Path;
use File::Spec;

if( eval "use VCP; 1" ) {
    plan tests => 4;
}
else {
    plan skip_all => 'VCP not installed';
}

my $m;

my $repospath = "t/repos";
rmtree ([$repospath]) if -d $repospath;

my $abs_path = File::Spec->rel2abs( $repospath ) ;

my $cvsroot = File::Spec->rel2abs( "t/cvs-test-data" ) ;
$m = SVN::Mirror->new (target_path => 'cvs-trunk',
		       repospath => $abs_path,
		       repos_create => 1,
		       options => ['--branch-only=trunk'],
		       source => "cvs:$cvsroot:kuso/...");
$m->init;
$m = SVN::Mirror->new (target_path => 'cvs-trunk',
		       repospath => $abs_path,
		       repos_create => 1,
		       get_source => 1);
$m->init;
is (ref $m, 'SVN::Mirror::VCP');
is ($m->{source}, "cvs:$cvsroot:kuso/...");
$m->run;
ok(1);
# check '/cvs-trunk/blah/more'

$m = SVN::Mirror->new (target_path => 'cvs-all', repospath => $abs_path,
		       source => "cvs:$cvsroot:kuso/...");
$m->init;
$m->run;
ok(1);
# check '/cvs-all/trunk/more'
