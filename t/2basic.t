#!/usr/bin/perl
use Test::More qw(no_plan);
use SVN::Mirror;
use File::Path;
use File::Spec;
use strict;

exit 0 unless -x "/usr/local/bin/svnadmin";

my $repospath = "t/repos";

rmtree ([$repospath]) if -d $repospath;


`svnadmin create $repospath`;

my $abs_path = File::Spec->rel2abs( $repospath ) ;
`svn mkdir -m 'init' file://$abs_path/source`;
`svnadmin load --parent-dir source $repospath < t/test_repo.dump`;

my $m = SVN::Mirror->new(target_path => 'fullcopy', target => $abs_path,
			 source => "file://$abs_path/source", target_create => 1);
$m->init ();
$m->run ();

ok(1);

$m = SVN::Mirror->new(target_path => 'partial', target => $abs_path,
		      source => "file://$abs_path/source/svnperl_002", target_create => 1);
$m->init ();
$m->run ();

ok(1);

END {
print `svn log -v file://$abs_path`;
}
