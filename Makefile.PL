#!/usr/bin/perl
my $required_svn = '0.33.0';

use inc::Module::Install;

name		('SVN-Mirror');
author		('Chia-liang Kao <clkao@clkao.org>');
abstract	('Mirror remote repositories to local subversion repository');
license		('perl');
version_from	('lib/SVN/Mirror.pm');
install_script	('bin/svm');

requires(
        'SVN::Core'   => $required_svn,
        'URI::Escape' => '0',
);
features(
    'VCP support' => [
	-default => 1,
        'VCP' => '0.9',
        'SVK' => '0.14',
        'Data::UUID' => '0',
    ],
);

include('ExtUtils::AutoInstall');
auto_install();

WriteAll( sign => 1 );

die "SVN::Core $required_svn must be manually installed"
    unless eval {require SVN::Core} && $SVN::Core::VERSION >= $required_svn;
