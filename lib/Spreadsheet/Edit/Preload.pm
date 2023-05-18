# License: http://creativecommons.org/publicdomain/zero/1.0/
# (CC0 or Public Domain).  To the extent possible under law, the author, 
# Jim Avera (email jim.avera at gmail) has waived all copyright and 
# related or neighboring rights to this document.  Attribution is requested
# but not required.

use strict; use warnings FATAL => 'all'; use feature qw/say state/;
use utf8;

package Spreadsheet::Edit::Preload;
# VERSION from Dist::Zilla::Plugin::OurPkgVersion
# DATE from Dist::Zilla::Plugin::OurDate

use Carp;
use Import::Into;
require Spreadsheet::Edit;
our @CARP_NOT = ('Spreadsheet::Edit');

sub import {
#use Data::Dumper::Interp; say dvis '###Preload import @_';
  my $pkg = shift;  # that's us

  my $callpkg = caller($Exporter::ExportLevel);

  # Import Spreadsheet::Edit and the usual variables for the user
  Spreadsheet::Edit->import::into($callpkg, ':all');

  # Load the spreadsheet
  my $sh = Spreadsheet::Edit->new->read_spreadsheet(@_);
#use Data::Dumper::Interp; say visnew->Objects(0)->dvis('###Preload GOT $sh');

  # Tie variables in the caller's package
  $sh->tie_column_vars({package => $callpkg}, ':all', ':safe');

  # Make it the 'current sheet' in the caller's package
  Spreadsheet::Edit::sheet( {package => $callpkg}, $sh );
}

1;
__END__
=pod

=encoding utf8

=head1 NAME

Spreadsheet::Edit::Preload - load and auto-import column variables

=head1 SYNOPSIS

  use Spreadsheet::Edit::Preload {OPTIONS}, PATH

  use Spreadsheet::Edit::Preload 
    {sheet => "Sheet1", title_rx => 2}, "/path/to/file.xls" ;

  apply {
    say "row ",($rx+1)," has $FIRST_NAME $LAST_NAME";
  };

  say "Row 4, column B contains ", $rows[3]{B};
  say "Row 4: "First Name" is ", $rows[3]{"First Name"};
  say "Row 4: "Last Name" is ", $rows[3]{Last_Name};
  say "There are ", scalar(@rows), " rows of data.";
  say "There are $num_cols columns";

=head1 DESCRIPTION

This is a wrapper for C<Spreadsheet::Edit> which loads a spreadsheet
at compile time.  Tied variables are imported having names derived
from column titles or letter codes; these may be used during "apply"
operations to access the corresponding column in the "current row".

The example above is equivalent to

  use Spreadsheet::Edit qw(:FUNCS :STDVARS);
  BEGIN {
    read_spreadsheet {sheet => "Sheet1"}, "/path/to/file.xls";
    title_rx 2;
    tie_column_vars ':all';
  }

You need not (and may not) explicitly declare the tied variables.

=head1 OPTIONS

The {OPTIONS} hashref is optional and may specify a workbook sheetname,
CSV parsing options, etc. (see I<read_spreadsheet> in L<Spreadsheet::Edit>).

   title_rx => ROWINDEX

explicitly specifies the 0-based row index of the title row.  If not
specified, the title row is auto-detected.

=head1 SECURITY

A fatal error occurs if a column letter ('A', 'B' etc.), a title, 
or identifier derived from the title (any COLSPEC)
clashes with an object already existing in the caller's package,
or in package main.

=head1 SEE ALSO

Spreadsheet::Edit

=cut
