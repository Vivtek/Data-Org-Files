#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;
use Data::Dumper;
$Data::Dumper::Useqq = 1;

use Data::Org::Files;

my $dm = Data::Org::Files->new (
   directory => 't/testdir',
   no_db => 1,
);

my $content_checker = $dm->iterator->select('path', 'ext', 'size');
is_deeply ($content_checker->load,
 [
   ['test1.txt',           'txt',    0],
   ['test2.txt',           'txt',    0],
   ['test3.nottxt',        'nottxt', 0],
   ['z_subdir/subdir.txt', 'txt',    0],
 ]);

is_deeply ($dm->search ()->select('rowid', 'path', 'ext', 'size')->load,
 [
   [1, 'test1.txt',           'txt',    0],
   [2, 'test2.txt',           'txt',    0],
   [3, 'test3.nottxt',        'nottxt', 0],
   [4, 'z_subdir/subdir.txt', 'txt',    0],
 ]);

my $fh;
my $id;
($fh, $id) = $dm->create;
printf $fh "This is text!";
$dm->create_close($id);

is_deeply ($content_checker->load,
 [
   ['file5', '', 13],
   ['test1.txt', 'txt', 0],
   ['test2.txt', 'txt', 0],
   ['test3.nottxt', 'nottxt', 0],
   ['z_subdir/subdir.txt', 'txt',    0],
 ]);

is_deeply($dm->search ()->select('rowid', 'path', 'ext', 'size')->load,
 [
   [5, 'file5',                undef,   13],
   [1, 'test1.txt',           'txt',    0 ],
   [2, 'test2.txt',           'txt',    0 ],
   [3, 'test3.nottxt',        'nottxt', 0 ],
   [4, 'z_subdir/subdir.txt', 'txt',    0 ],
 ]);
 
eval { $fh = $dm->retrieve(10000); };
like ( $@, qr/not found in store/ );

$fh = $dm->retrieve($id);
my $text = <$fh>;
close $fh;
is ($text, "This is text!");

$text = $dm->retrieve_all($id);
is ($text, "This is text!");

$fh = $dm->update ($id);
print $fh "Different text...\n";
close $fh;
$text = $dm->retrieve_all($id);
is ($text, "Different text...\n");

$fh = $dm->append ($id);
print $fh "Second line\n";
close $fh;
$text = $dm->retrieve_all($id);
is ($text, "Different text...\nSecond line\n");

$dm->delete ($id);
is_deeply ($content_checker->load,
 [
   ['test1.txt',           'txt',    0 ],
   ['test2.txt',           'txt',    0 ],
   ['test3.nottxt',        'nottxt', 0 ],
   ['z_subdir/subdir.txt', 'txt',    0 ],
 ]);

is_deeply($dm->search ()->select('rowid', 'path', 'ext', 'size')->load,
 [
   [1, 'test1.txt',           'txt',    0 ],
   [2, 'test2.txt',           'txt',    0 ],
   [3, 'test3.nottxt',        'nottxt', 0 ],
   [4, 'z_subdir/subdir.txt', 'txt',    0 ],
 ]);

# Now do create, update, append from files.
$id = $dm->create_from ('t/testfile.txt');
is_deeply ($content_checker->load,
 [
   ['file5', '', 24],
   ['test1.txt', 'txt', 0],
   ['test2.txt', 'txt', 0],
   ['test3.nottxt', 'nottxt', 0],
   ['z_subdir/subdir.txt', 'txt',    0 ],
 ]);
 
$text = $dm->retrieve_all($id);
is ($text, "This is a line of text.\n");

$dm->append_from ($id, 't/testfile.txt');
$text = $dm->retrieve_all($id);
is ($text, "This is a line of text.\nThis is a line of text.\n");

$dm->update_from ($id, 't/testfile.txt');
$text = $dm->retrieve_all($id);
is ($text, "This is a line of text.\n");
 
$dm->delete($id);

done_testing();
