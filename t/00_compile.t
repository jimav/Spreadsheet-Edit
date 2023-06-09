#!/usr/bin/env perl
use strict;
use Test2::V0;

use Spreadsheet::Edit;

use Spreadsheet::Edit::IO qw/let2cx cx2let convert_spreadsheet
                             filepath_from_spec sheetname_from_spec/;

{ my $path; eval{ $path = Spreadsheet::Edit::IO::_openlibre_path() };
  is(!!Spreadsheet::Edit::IO::spreadsheets_ok(), !!$path);
  if (!$path) {
    die "$@ " if $@ and $@ !~ /not find.*Libre/i;
    diag "LibreOffice is not installed\n";
  }
  diag "_openlibre_path : $path";
}

ok(1, "Basic loading & import");

done_testing;

