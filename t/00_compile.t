#!/usr/bin/perl
use FindBin qw($Bin);
use lib $Bin;
use t_Common qw/oops/; # strict, warnings, Carp
use t_TestCommon # Test2::V0 etc.
  qw/$silent $verbose $debug run_perlscript/;
use Capture::Tiny qw/capture/;

use Spreadsheet::Edit;

use Spreadsheet::Edit::IO qw/let2cx cx2let convert_spreadsheet
                             filepath_from_spec sheetname_from_spec/;

# On Solars and maybe others File::Find warns "Use of uninitialized..."
# while _openlibre_path searches for an installation.
# Enable debug tracing but only show it if there is a problem
my $path;
{ my @warnings;
  eval{
    local $SIG{__WARN__} = $debug ? 'DEFAULT' : sub { push @warnings, @_; };
    local $ENV{SPREADSHEET_EDIT_FINDDEBUG} = 1;
    $path = Spreadsheet::Edit::IO::_openlibre_path();
  };
  my $caught = $@;
  if ($caught || any { /se of uninitialized/} @warnings) {
    diag @warnings;
    diag $caught if $caught;
    fail("File::Find trouble") unless $debug && !$caught;
  }
}
is(!!Spreadsheet::Edit::IO::spreadsheets_ok(), !!$path);
if (!$path) {
  diag "LibreOffice not found\n";
} else {
  diag "_openlibre_path : $path";
}

ok(1, "Basic loading & import; find LibreOffice");

done_testing;

