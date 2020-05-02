#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;
use Data::Dumper;
$Data::Dumper::Useqq = 1;

use Data::Org::Files;

# Next mode: custom iterator.
my $dm = Data::Org::Files->new (
   directory => 't/testdir',
);

my $content_checker = $dm->iterator->select('path', 'ext', 'size');
is_deeply ($content_checker->load,
 [
   ['test1.txt', 'txt', 0],
   ['test2.txt', 'txt', 0],
   ['test3.nottxt', 'nottxt', 0],
   ['z_subdir/subdir.txt', 'txt', 0 ],
 ]); # Note: does not include database file even though it's in this directory.

is_deeply ($dm->{cols}, ['path', 'name', 'ext', 'filetype', 'modestr', 'size', 'uid', 'gid', 'mtime']);
$dm->load_db;
is_deeply ($dm->search->select('path', 'ext', 'size')->load,
 [
   ['test1.txt', 'txt', 0],
   ['test2.txt', 'txt', 0],
   ['test3.nottxt', 'nottxt', 0],
   ['z_subdir/subdir.txt', 'txt', 0 ],
 ]);

unlink ('t/testdir/docmgt.sqlt');
done_testing();
