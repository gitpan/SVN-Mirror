#!/usr/bin/perl
use Test::More tests => 4;
use SVN::Mirror;
use File::Path;
use File::Spec;
use strict;

exit 0 unless -x '/usr/local/bin/svnadmin' || -x '/usr/bin/svnadmin';

my $repospath = "t/repos";

rmtree ([$repospath]) if -d $repospath;


`svnadmin create $repospath`;

my $abs_path = File::Spec->rel2abs( $repospath ) ;
`svn mkdir -m 'init' file://$abs_path/source`;
`svnadmin load --parent-dir source $repospath < t/test_repo.dump`;

my $m = SVN::Mirror->new(target_path => 'fullcopy', target => $abs_path,
			 source => "file://$abs_path/source", target_create => 1);
is (ref $m, 'SVN::Mirror::Ra');
$m->init ();

$m = SVN::Mirror->new (target_path => 'fullcopy', target => $abs_path,
		       get_source => 1,);

is ($m->{source}, "file://$abs_path/source");
$m->init ();
$m->run ();

ok(1);

$m = SVN::Mirror->new(target_path => 'partial', target => $abs_path,
		      source => "file://$abs_path/source/svnperl_002", target_create => 1);
$m->init ();
$m->run ();

ok(1);
