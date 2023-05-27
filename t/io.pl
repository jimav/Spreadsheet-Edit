#!/usr/bin/perl
# *** DO NOT USE Test2 FEATURES becuase this is a sub-script ***
use FindBin qw($Bin);
use lib $Bin;
use t_Common qw/oops/; # strict, warnings, Carp etc.

use t_TestCommon  # Test::More etc.
         qw/$verbose $silent $debug dprint dprintf
            bug mycheckeq_literal expect1 mycheck 
            verif_no_internals_mentioned
            insert_loc_in_evalstr verif_eval_err
            arrays_eq hash_subset
            @quotes/;

use t_SSUtils;
use Capture::Tiny qw/capture_merged tee_merged/;

use Spreadsheet::Edit qw/:all logmsg fmt_sheet cx2let let2cx sheet/;
use Spreadsheet::Edit::IO qw/convert_spreadsheet/;

use Test::Deep::NoTest qw/eq_deeply/;

{ my $path = eval{ Spreadsheet::Edit::IO::_openlibre_path() };
  if (!$path && $@ =~ /not find.*Libre/i) {
    say __FILE__,": Skipping all because LibreOffice is not installed";
    exit 0
  }
  die "$@ " if $@;
  say "Using $path" unless $silent;
}

my $cwd = fastgetcwd;
my $input_xlsx_path = abs2rel(fast_abs_path("$Bin/../tlib/Test.xlsx"), $cwd);

sub verif_Sheet1(;$){
  my $msg = $_[0] // "";
  eq_deeply(title_rx(), 0) or die "${msg} title_rx is not 0";
  eq_deeply([@{ title_row() }],["First Name","Last Name","Email","Date"])
    or die "${msg} Sheet1 titles wrong";
}
sub verif_Another_Sheet(;$) {
  my $msg = $_[0] // "";
  eq_deeply(title_rx(), 0) or die "${msg} title_rx is not 0";
  eq_deeply([@{ title_row() }],["Input","Output"])
    or die "Another Sheet titles wrong; title_row...",vis([@{ title_row() }]);
  apply {
    my $exp = 100 + $rx - 1;
    eq_deeply($crow{B}, $exp) or die "$msg Another Sheet:Col B, rx $rx is $exp";
  }
}
sub doread($$) {
  my ($opts, $inpath) = @_;
  sheet undef;
  eq_deeply(sheet(), undef) or die "sheet() did not return undef";
  eq_deeply(eval{$num_cols},undef) or die "num_cols unexpectedly valid";
  read_spreadsheet {debug => $debug, verbose => $verbose, %$opts}, $inpath;
  die "num_cols is not positive" unless $num_cols > 0;
}
# Test the various ways of specifying a sheet name
doread({}, $input_xlsx_path."!Sheet1"); verif_Sheet1();
doread({sheetname => "Sheet1"}, $input_xlsx_path); verif_Sheet1;
doread({sheetname => "Sheet1"}, $input_xlsx_path."!Sheet1"); verif_Sheet1;
doread({sheetname => "Another Sheet"}, $input_xlsx_path."!Another Sheet"); verif_Another_Sheet;

# Confirm that conflicting specs are caught
eval{ read_spreadsheet {sheetname => "Sheet1", verbose => $verbose}, $input_xlsx_path."!Another Sheet" };
die "Conflicting sheetname opt and !suffix not caught" if $@ eq "";

# Extract all sheets
my $dirpath = Path::Tiny->tempdir;
convert_spreadsheet(outpath => $dirpath, allsheets => 1, inpath => $input_xlsx_path, cvt_to => "csv", verbose => $verbose, debug => $debug);
say dvis '###BBB $dirpath ->children : ',avis($dirpath->children) if $debug;
my $got = [sort map{$_->basename} $dirpath->children];
my $exp = [sort "Sheet1.csv", "Another Sheet.csv"];
eq_deeply($got, $exp) or die dvis 'Missing or extra sheets: $got $exp';

# "Read" a csv; should be a pass-thru without conversion
read_spreadsheet {verbose => $verbose}, $dirpath->child("Sheet1.csv");
verif_Sheet1 "(extracted csv)";

#TODO: convert_spreadsheet csv->csv with transcoding

say "Done." unless $silent;
exit 0;

