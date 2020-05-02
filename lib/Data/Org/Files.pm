package Data::Org::Files;

use 5.006;
use strict;
use warnings;
use Path::Tiny;
use Iterator::Records;
use Iterator::Records::Files;
use Data::Dumper;
use List::Util qw(any);
use Carp;


=head1 NAME

Data::Org::Files - Filesystem-based document management

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Data::Org::Files builds on L<Iterator::Records> to provide a document manager for files in the file system. This document management functionality can
be as simple as a searchable iterator over a directory (optionally recursing into subdirectories), but it can also include a database index of those
files to store metadata and to track changes to the directory if the directory is used by other tools. It can also be used to manage private directories of
files intended to be accessed only through its API.

Although it's part of the Data::Org ecosystem, Data::Org::Files has no dependencies on Data::Org, so you can use it independently.

The API for this module is the standard API for all Data::Org document managers, so they're all pluggable for different situations.

=head1 OPENING A DOCUMENT MANAGER

=head2 new (options)

The options here are a hash of the following:

=over

=item directory

The directory to be managed. If not provided, the current directory will be assumed.

=item iterator

A file iterator to be wrapped for document management. If you don't want to build your own iterator, Data::Org::Files will build a default iterator for
the managed directory.

=item create_name

A string to be used to build filenames when creating new files. A unique index will be added to the filename. No extension will be provided automatically,
but if your iterator supplies a "role" field, you can specify extensions in C<extensions>.

=item extensions

A hashref from role names to the extensions to use when creating a file of that role. The "*" role matches anything, and will be used if the iterator does
not supply a "role" field.

=item no_db

The default is to open and use an SQLite database for tracking the managed directory, but you can suppress this with a true value for C<no_db>.

=item db

The name of an SQLite database file to use for management of the directory. The default (used if neither C<db> nor C<no_db> are specified) is "docmgt.sqlt".
The file is relative to the managed directory, and the iterator will be wrapped to skip the database file when iterating.

=item dbh

If you're not using SQLite, you'll need to connect to it. Then pass its handle in here.

=item dbcontrol

If a database is specified, there are two modes possible: either the directory controls the docmgr (i.e. external changes to that directory are tracked
using the database), or the database controls the directory (no external changes are allowed, although there's no way to enforce that). The default is
directory control, so specify a true value for C<dbcontrol> if you want the database to control the situation.

A document manager with directory control will use the directory iterator to iterate files; a database-controlled docmgr will simply iterate the database
table.

=item table (default: "files")
=item cols (default: id + the iterator fields)
=item no_id (flag)
=item datatab (default: none)
=item datatab_cols (default: id, key, value)

=back

=cut

sub new {
   my $class = shift;
   my %parms = @_;
   
   my $self = bless {}, $class;
   $self->{directory}    = $parms{directory}    || '.';
   $self->{create_fname} = $parms{create_fname} || \&_create_fname;
   $self->{no_db}        = $parms{no_db}        || 0;
   if ($self->{no_db}) {
      $self->{dbh} = Iterator::Records::db->open();
   } else {
      $self->{database}  = $parms{database}     || path($self->{directory}, 'docmgt.sqlt')->stringify;
      $self->{dbh}       = Iterator::Records::db->open($self->{database}); # $parms{dbh}          || Iterator::Records::db->open($self->{database});
   }
   $self->{table}     = $parms{table}        || 'files';
   $self->{coldef}    = $parms{coldef}       || "path, name, ext, filetype, modestr, size integer, uid integer, gid integer, mtime integer";
   $self->{cols}      = $parms{cols}         || undef;
   $self->_verify_db;
   
   $self->{iterator}     = $parms{iterator}     || $self->_build_dir_iter ($self->{directory});
   $self->load_db if $self->{no_db}; # A persistent database is presumed to still be valid from last time, but an in-mem database needs to be loaded.
   
   $self;
}

sub _build_dir_iter {
   my $self = shift;
   my $dirname = shift || '.';
   my $it = Iterator::Records::Files->walk ($dirname, {sorted => 1, clean => 1})
            ->where  (sub {$_[0] eq '-'}, 'filetype')
            ->rename ('path', 'relpath')
            ->calc   (sub { my $s = shift; $s =~ s/^$dirname\///; $s }, 'path', 'relpath')
            ->select ('path', 'name', 'ext', 'filetype', 'modestr', 'size', 'uid', 'gid', 'mtime');
   if (not $self->{no_db} and $self->{database}) {
      my $db_path = path($self->{database})->realpath;
      my $dirpath = path($self->{directory})->realpath;
      if ($dirpath->subsumes($db_path)) { # Oh, Path::Tiny, where have you been all my life?
         my $dbname = $db_path->basename;
         $it = $it->where(sub { $_[0] ne $dbname }, 'name');  # If the database is in the managed directory, this new iterator won't show it.
      }
   }
   $it;
}

sub _verify_db {
   my $self = shift;
   # Does the table exist and does it have the columns we expect?
   my $sth = $self->{dbh}->column_info(undef, undef, $self->{table}, undef);
   my $array = $sth->fetchall_arrayref;
   if (not scalar @$array) {
       $self->_create_table unless scalar @$array;
       $sth = $self->{dbh}->column_info(undef, undef, $self->{table}, undef);
       $array = $sth->fetchall_arrayref;
   }
   my @cols;
   foreach my $row (@$array) {
      push @cols, $row->[3];
   }
   if (defined $self->{cols}) {
      # Verify that all the columns we will be returning are actually defined in the table.
   } else {
      $self->{cols} = \@cols;
   }
   #printf STDERR Dumper(\@cols);
}
sub _create_table {
   my $self = shift;
   $self->{dbh}->do("create table " . $self->{table} . " (" . $self->{coldef} . ");");
}

=head1 CRUD FUNCTIONALITY

The core of document management is of course to be able to create, retrieve, update, and delete documents in a store, and to attach arbitrary metadata values to
those documents. A straight no-db filesystem document manager (i.e. a dumb directory) can't store metadata, but some of the statd information for the file is treated
as (constant) metadata.

=cut

sub create {
   my $self = shift;
   my $metadata = shift;

   my $id = $self->{dbh}->insert($self->_make_insert_from_metadata($metadata));
   my $file = $self->{create_fname}->($self, $id, $metadata);
   $self->_update_db ($id, { name => $file, path => $file }, 'name', 'path');
   my $fh = path($self->{directory}, $file)->openw or croak "Can't open created document: $!";
   $self->{filehandles}->{$id} = [$fh, $file];
   return wantarray ? ($fh, $id) : $fh;
}
sub create_close {
   my $self = shift;
   my $id = shift;

   return unless $self->{filehandles};
   return unless $self->{filehandles}->{$id};
   my ($fh, $fname) = @{$self->{filehandles}->{$id}};
   delete $self->{filehandles}->{$id};

   $fh->close;
   $self->_update_db_from_file($id, $fname);
   
}
sub create_from {
   my $self = shift;
   my $source = shift;
   my $metadata = shift;
   
   $source = path($source);
   croak "$source does not exist" unless $source->exists;
   croak "$source is not a file" unless -f $source;
   
   my $id = $self->{dbh}->insert($self->_make_insert_from_metadata($metadata));
   my $file = $self->{create_fname}->($self, $id, $metadata);
   $self->_update_db ($id, { name => $file, path => $file }, 'name', 'path');
   #$self->{dbh}->do ("update " . $self->{table} . " set name=? where rowid=?", $file, $id);
   $source->copy(path($self->{directory}, $file));
   $self->_update_db_from_file ($id, $file);
   return $id;
}

sub _update_db {
   my $self = shift;
   my $id = shift;
   my $values = shift;
   
   my @ufields = map { $_ . "=?" } @_;
   $self->{dbh}->do ("update " . $self->{table} . " set " . join (', ', @ufields) . " where rowid=?", (map { $values->{$_} } @_), $id);
}
sub _update_db_from_file {
   my $self = shift;
   my $id = shift;
   my $fname = shift;
   
   my $fileinfo = Iterator::Records::Files->check_hash(path($self->{directory}, $fname)->stringify);
   $self->_update_db ($id, $fileinfo, 'ext', 'filetype', 'modestr', 'size', 'uid', 'gid', 'mtime');
}
sub _create_fname {
   my $self = shift;
   my $id = shift;
   my $metadata = shift;

   if ($metadata && $metadata->{filename}) {
      return $metadata->{filename};
   }

   my $fname = "file" . $id;
   my $offset = "0";
   my $ffname = $fname;
   while ($self->_check_unique_filename($ffname)) {
      $offset += 1;
      $ffname = $fname . "_" . $offset;
   }
   return $ffname;
}
sub _check_unique_filename {
   my $self = shift;
   my $fname = shift;
   return path($self->{directory}, $fname)->exists;
}
sub _make_insert_from_metadata {
   my $self = shift;
   my $metadata = shift;
   my @inserts = map { '?' } @{$self->{cols}};
   return "insert into " . $self->{table} . " values (" . join (', ', @inserts) . ')';

}
sub _find_unused_filename { # TODO: this is hopelessly naive and will lead to race conditions if stressed even a little, but a non-db docmgr is not intended to scale.
   my $self = shift;
   my $file;
   my $path;

   my $offset = 0;   
   do {
      $offset += 1;
      $file = sprintf ("%s%05d", $self->{create_fname}, $offset); 
      $path = path ($self->{directory}, $file);
   } while ($path->exists);
   return ($file, $path);
}

sub _get_storage_path {
   my $self = shift;
   my $id = shift;
   my $file = $self->{dbh}->get("select path from " . $self->{table} . " where rowid=?", $id);
   croak "ID $id not found in store" unless $file;
   my $path = path ($self->{directory}, $file);
   croak "File not found for ID $id" unless $path->is_file;
   return $path;
}

sub retrieve {
   my $self = shift;
   my $id = shift;
   my $path = $self->_get_storage_path($id);
   return $path->openr;
}
sub retrieve_all {
   my $self = shift;
   my $id = shift;
   my $path = $self->_get_storage_path($id);
   return $path->slurp;
}

sub update {
   my $self = shift;
   my $id = shift;
   my $path = $self->_get_storage_path($id);

   my $fh = $path->openw or croak "Can't open ID $id: $!";
   $self->{filehandles}->{$id} = [$fh, $path];
   return $fh;
}
sub update_close {
   my $self = shift;
   my $id = shift;

   return unless $self->{filehandles};
   return unless $self->{filehandles}->{$id};
   my ($fh, $fname) = @{$self->{filehandles}->{$id}};
   delete $self->{filehandles}->{$id};

   $fh->close;
   $self->_update_db_from_file($id, $fname);
}

sub update_from {
   my $self = shift;
   my $id = shift;
   my $source = shift;

   my $path = $self->_get_storage_path($id);
   
   $source = path($source);
   croak "$source does not exist" unless $source->exists;
   croak "$source is not a file" unless -f $source;
   
   $source->copy($path);
   $self->_update_db_from_file($id, $path);
}
sub append {
   my $self = shift;
   my $id = shift;
   my $path = $self->_get_storage_path($id);

   my $fh = $path->opena or croak "Can't open ID $id: $!";
   $self->{filehandles}->{$id} = [$fh, $path];
   return $fh;
}
sub append_close { update_close(@_) }
sub append_from {
   my $self = shift;
   my $id = shift;
   my $source = shift;
   
   my $path = $self->_get_storage_path($id);

   $source = path($source);
   croak "$source does not exist" unless $source->exists;
   croak "$source is not a file" unless -f $source;
   
   $path->append ($source->slurp);
   $self->_update_db_from_file($id, $path);
}


sub delete {
   my $self = shift;
   my $id = shift;
   
   my $path = $self->_get_storage_path($id);
   $path->remove;
   $self->{dbh}->do ("delete from " . $self->{table} . " where rowid=?", $id);
}

=head2 LOADING INFORMATION

The filesystem document store is (well, might be) unique because it splits storage between a directory and a database table that can act in a tracking capacity.
In other words, we need a mechanism to look at the directory and adjust the database. That's "scan". It only makes sense for stores that split authority between
two storage locations like this.

=head2 load_db

The C<load_db> method is just used for initial loading of the database. Delta tracking (checking the existing directory against the stored information we have)
will be a later phase of development.

=cut

sub load_db {
   my $self = shift;
   my $it = $self->{iterator};
   my $itfields = $it->fields;
   my @fields;
   foreach my $f (@{$self->{cols}}) {
      push @fields, $f if any { $_ eq $f } @$itfields;
   }
   $self->{dbh}->do ('delete from ' . $self->{table});
   $self->{dbh}->load_table ($self->{table}, $it->select (@fields));
}

=head1 ITERATION AND SEARCH

The next step beyond a mere CRUD store manager is to be able to iterate over the documents in the store.

=head2 iterator

Returns the file record iterator used to load (or reload) the database.

=cut

sub iterator { $_[0]->{iterator} }

=head2 iterate

Returns a started iterator if you're in a hurry and just want to iterate some stuff.

=cut

sub iterate { $_[0]->{iterator}->iter }

=head2 search ([where spec], [order fields])

This takes the where clause for a query to be run against the file database. Returns a record iterator. If the where clause is omitted, returns all rows
in the document store. Default order is by path (to allow scanning to work), but the "order" field can be supplied with a list of fields to order by, either
as a single string of comma-delimited fields or simply in an arrayref.

=cut

sub search {
   my $self = shift;
   my $where = shift;
   my $order = shift || 'path';
   if (ref $order) {
      $order = join (', ', @$order);
   }
   $self->{dbh}->iterator("select rowid, * from " . $self->{table} . (defined $where ? " where $where" : '') . " order by $order");
}

=head1 AUTHOR

Michael Roberts, C<< <michael at vivtek.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-data-org-files at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Data-Org-Files>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Data::Org::Files


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Data-Org-Files>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Data-Org-Files>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Data-Org-Files>

=item * Search CPAN

L<http://search.cpan.org/dist/Data-Org-Files/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2020 Michael Roberts.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1; # End of Data::Org::Files
