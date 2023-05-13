#!/usr/bin/perl
use FindBin qw($Bin);
use lib $Bin;
use t_Common qw/oops mytempfile mytempdir/; # strict, warnings, Carp etc.

use t_TestCommon  # Test::More etc.
         qw/$verbose $silent $debug dprint dprintf
            bug mycheckeq_literal expect1 mycheck 
            verif_no_internals_mentioned
            insert_loc_in_evalstr verif_eval_err
            arrays_eq hash_subset
            string_to_tempfile
            @quotes/;

use t_SSUtils;
use Capture::Tiny qw/capture_merged tee_merged/;

use Spreadsheet::Edit qw/:all logmsg fmt_sheet cx2let let2cx sheet/;
use Spreadsheet::Edit::IO qw/convert_spreadsheet/;

my $cwd = fastgetcwd;
my $input_xlsx_path = abs2rel(fast_abs_path("$Bin/../tlib/Test.xlsx"), $cwd);

sub verif_Sheet1(;$){
  my $msg = $_[0] // "";
  is(title_rx(), 0, "${msg} title_rx is 0");
  is([@{ title_row() }],["First Name","Last Name","Email","Date"], "${msg} Sheet1 titles");
}
sub verif_Another_Sheet(;$) {
  my $msg = $_[0] // "";
  is(title_rx(), 0, "$msg title_rx is 0");
  is([@{ title_row() }],["Input","Output"], "$msg Another Sheet titles");
  apply {
    my $exp = 100 + $rx - 1;
    is($crow{B}, $exp, "$msg Another Sheet: Col B, rx $rx is $exp");
  }
}
sub doread($$) {
  my ($opts, $inpath) = @_;
  sheet undef;
  is(sheet(), undef);
  is(eval{$num_cols}, undef);
  read_spreadsheet {debug => 0, %$opts}, $inpath;
  ok($num_cols > 0, dvis('$opts $inpath'));
}
# Test the various ways of specifying a sheet name
doread({}, $input_xlsx_path."!Sheet1"); verif_Sheet1();
doread({sheetname => "Sheet1"}, $input_xlsx_path); verif_Sheet1;
doread({sheetname => "Sheet1"}, $input_xlsx_path."!Sheet1"); verif_Sheet1;
doread({sheetname => "Another Sheet"}, $input_xlsx_path."!Another Sheet"); verif_Another_Sheet;

# Confirm that conflicting specs are caught
eval{ read_spreadsheet {sheetname => "Sheet1"}, $input_xlsx_path."!Another Sheet" };
like($@, qr/conflict.*sheet/, "Conflicting sheetname opt and !suffix");

# Extract all sheets
my $dirpath = mytempdir();
convert_spreadsheet(outpath => $dirpath, allsheets => 1, inpath => $input_xlsx_path,
                    cvt_to => "csv");
for my $fname ("Sheet1.csv", "Another Sheet.csv") {
  ok(-r catfile($dirpath, $fname), "'$fname' was produced");
}
read_spreadsheet catfile($dirpath,"Sheet1.csv");
verif_Sheet1 "(extracted csv)";

done_testing();
exit 0;

