#!/usr/bin/perl
use ExtUtils::MakeMaker;

my $required_svn = '0.33.0';

WriteMakefile(
    AUTHOR              => 'Chia-liang Kao (clkao@clkao.org)',
    ABSTRACT            => 'Mirror remote subversion repositories',
    NAME                => 'SVN::Mirror',
    VERSION_FROM        => 'Mirror.pm',
    DISTNAME            => 'SVN-Mirror',
    PREREQ_PM           => {
        'SVN::Core'   => $required_svn,
        'URI::Escape' => '0',
    },
    EXE_FILES    => [ 'bin/svm' ],
    dist                => {
        COMPRESS        => 'gzip -9',
        SUFFIX          => '.gz',
    },
);

die "SVN::Core $required_svn must be manually installed"
    unless eval {require SVN::Core} && $SVN::Core::VERSION >= $required_svn;
