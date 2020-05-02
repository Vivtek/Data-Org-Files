#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Data::Org::Files' ) || print "Bail out!\n";
}

diag( "Testing Data::Org::Files $Data::Org::Files::VERSION, Perl $], $^X" );
