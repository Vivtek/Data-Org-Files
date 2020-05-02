#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;
use Data::Dumper;
use Iterator::Records::Files;
$Data::Dumper::Useqq = 1;

use Data::Org::Files;

# Next mode: custom iterator.
# Note: a non-walk iterator has to be fixed up with a 'path' column using a calc transmogrifier.
my $special_iterator = Iterator::Records::Files->readdir_q ('t/testdir', {sorted=>1, clean=>1})->where(sub { $_[0] eq 'txt' }, 'ext')->calc(sub { $_[0] }, 'path', 'name');
my $dm = Data::Org::Files->new (
   directory => 't/testdir',
   iterator => $special_iterator,
   no_db => 1,
);

my $content_checker = $dm->iterator->select('name', 'ext', 'filetype', 'size');
is_deeply ($content_checker->load,
 [
   ['test1.txt', 'txt', '-', 0],
   ['test2.txt', 'txt', '-', 0],
 ]);

# Alternatively, we can force a particular set of columns explicitly in order to match the iterator.
$special_iterator = Iterator::Records::Files->readdir_q ('t/testdir', {sorted=>1, clean=>1})->where(sub { $_[0] eq 'txt' }, 'ext');
$dm = Data::Org::Files->new (
   directory => 't/testdir',
   iterator => $special_iterator,
   coldef => "name, ext, filetype, modestr, size integer, uid integer, gid integer, mtime integer",
   no_db => 1,
);

$content_checker = $dm->iterator->select('name', 'ext', 'filetype', 'size');
is_deeply ($content_checker->load,
 [
   ['test1.txt', 'txt', '-', 0],
   ['test2.txt', 'txt', '-', 0],
 ]);
 

done_testing();
