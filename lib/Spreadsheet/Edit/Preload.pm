# Copyright © Jim Avera 2021.  Released into the Public Domain
# by the copyright owner.  (jim.avera AT gmailgmail  daht komm)
# Please retain the preceeding attribution in any copies or derivitives.

# $Id: Preload.pm,v 1.4 2021/10/08 17:05:07 jima Exp jima $

use strict; use warnings FATAL => 'all'; use v5.20;
use utf8;

package Spreadsheet::Edit::Preload;


use version 0.77; our $VERSION = version->declare(sprintf "v%s", q$Revision: 1.4 $ =~ /(\d[.\d]+)/);

use Carp;
use Import::Into;
require Spreadsheet::Edit;
our @CARP_NOT = ('Spreadsheet::Edit');

sub import {
  my $pkg = shift;  # that's us
  my ($inpath, %opts) = @_;
  my $callpkg = caller($Exporter::ExportLevel);

  # Import Spreadsheet::Edit and the usual variables for the user
  Spreadsheet::Edit->import::into($callpkg, ':DEFAULT', ':STDVARS');

  # Load the spreadsheet and tie column variable in caller's package
  my $sh = Spreadsheet::Edit->new(%opts);
  my $title_rx_arg = delete $opts{title_rx};
  $sh->read_spreadsheet($inpath, %opts);
  $sh->title_rx($title_rx_arg);  # undef if no titles 
  Spreadsheet::Edit::package_active_sheet($callpkg, $sh);
  # Actually safe need no longer be specified explicitly because it 
  # will be automatically assumed while ${^GLOBAL_PHASE} is "START".
  # See code in Spreadsheet::Edit::OO::tie_column_vars.
  $sh->tie_column_vars({package => $callpkg, safe => 1, %opts});
}

1;
__END__
=pod

=encoding utf8

=head1 NAME

Text::Csv::Edit::Preload - load and auto-import column variables

=head1 SYNOPSIS

  use Spreadsheet::Edit::Preload PATH, [OPTIONS...]

  use Spreadsheet::Edit::Preload "/path/to/file.xls", sheet => "Sheet1",
                               title_rx => 0;

  apply {
    say "row ",($rx+1)," has $FIRST_NAME $LAST_NAME";
  }

=head1 DESCRIPTION

A spreadsheet or csv file is immediately
loaded and tied variables corresponding to columns are created 
and imported into the caller's package.  These variables may
be used in C<apply> operations to access cells in the current row as
described in C<Spreadsheet::Edit>.

These imported variables have names derived from column titles 
(if C<title_rx> is specified), as well as $A, $B, etc. corresponding to
to absolute columns.

The example above is equivalent to

  use Spreadsheet::Edit qw(:DEFAULT :STDVARS);
  BEGIN {
    options ... ;  
    read_spreadsheet "/path/to/file.xls", sheet => "Sheet1";
    title_rx 0;
    tie_column_vars;
  }

Note that you need not (and may not) explicitly declare tied
column variables when importing them this way, in contrast with
normal usage (not in a BEGIN block) where the variables must be
declared with C<our> to be usable by your code.

=head1 OPTIONS

OPTIONS consist of key => value pairs.  They will be passed to
C<read_spreadsheet>, C<title_rx> or C<options> as appropriate.

=head1 SECURITY

If a title, or the identifier derived from the title, would clash with a
variable which already exists in the caller's package or in package main, 
then a warning is issued and the variable is not imported (package main
is checked to avoid clobbering special Perl variables such as C<STDOUT>).

In such cases the column can be accessed using the appropriate
spreadsheet-letter variable $A, etc. (assuming I<that> name does 
not clash with something).

=head1 SEE ALSO

Spreadsheet::Edit

=cut
