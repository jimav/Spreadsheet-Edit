#!/usr/bin/perl
use strict; use warnings  FATAL => 'all'; use feature qw(state say); use utf8;
BEGIN {
  srand(42);  # so reproducible
  STDERR->autoflush(1); STDOUT->autoflush(1);
}
use Carp;
use open IO => ':locale';
use List::Util qw(min max);
use Data::Dumper::Interp;

# This script was written before the author knew anything about standard
# Perl test-harness tools.  So almost everything is one giant test case.
use Test::More;

# TODO: Test join_cols & join_cols_sep

use Spreadsheet::Edit ':all';

sub verif_no_internals_mentioned($) {
  local $_ = shift;
  my $msg;
  # Ignore object refs like 'Package::REF=(hexaddr)'
  # Ignore anonymous file handles '\*{"Package:"\$fh"}'
  if (/(?<!\\\*\{")(?<hit>Spreadsheet::[\w:]*+(?!=REF\(|\\\$\w+))[^\w:]/s) { 
    $msg = "ERROR: Log msg or traceback mentions internal package '$+{hit}'"
  }
  elsif (/(?<hit>[-.\w\/+]\.pm\b)/s) {
    $msg = "ERROR: Log msg or traceback mentions file '$+{hit}' (presumably not user code)"
  }
  if ($msg) {
    my $start = $-[1]; # offset of start of item
    my $end   = $+[1]; # offset of end+1
    substr($_,$start,0) = "HERE>>>";
    substr($_,$end+7,0) = "<<<THERE";
    local $Carp::Verbose = 0;  # no full traceback 
    $Carp::CarpLevel++;
    croak $msg, ":\n«$_»\n";
  }
}

our ($debug, $verbose, $silent);
BEGIN {
  $silent = 1;
  my $internal;
  if (my ($ax) = grep{$ARGV[$_] =~ /^(?:-i|--internal)$/} 0..$#ARGV) {
    splice @ARGV, $ax, 1;
    $internal = 1;
  }
  my @rerun_cmd = ($^X, $Carp::Verbose ? ("-MCarp=verbose"):(), $0, @ARGV);
  while (@ARGV) {
    if ($ARGV[0] =~ /^(-d|--debug)/) { 
      $debug=$verbose=1; $silent=0; shift @ARGV }
    elsif ($ARGV[0] =~ /^(-v|--verbose)/)  { $verbose=1; shift @ARGV }
    elsif ($ARGV[0] =~ /^(-s|--silent)/)   
             { $silent=1; $verbose = $debug = 0; shift @ARGV }
    elsif ($ARGV[0] =~ /^(--no-silent)/)   { $silent=0; shift @ARGV }
    elsif ($ARGV[0] =~ /^--fail/) { die "fake failure" }
    else { die "$0: Unknown option: $ARGV[0]" }
  }
  if (!$internal) {
    use Capture::Tiny qw/capture_merged tee_merged/;

    use Test::More;
    plan tests => $debug ? 1 : 2;

    if (!$debug) {
      # Run normally; should be silent.
      my ($output, $wstat) = tee_merged { system @rerun_cmd, "--internal" };
      my @errs;
      push @errs, sprintf("** WAIT STATUS = 0x%04X\n", $wstat) if $wstat != 0;
      push @errs, "Expected no output (w/o verbose or debug)" if $output ne "";
      diag($_) foreach(@errs);
      ok (@errs == 0, "The whole shebang silently") or die;
    }
    {
      # Run with --debug; croaks should not show internal packages
      my ($output,$wstat) = capture_merged 
                              { system @rerun_cmd, "--internal", "--debug" };
      diag($output) if $wstat != 0;
      my @errs;
      eval{ verif_no_internals_mentioned($output) }; push @errs, $@ if $@;
      push @errs, sprintf("** WAIT STATUS = 0x%04X\n", $wstat) if $wstat != 0;
      diag($_) foreach(@errs);
      ok (@errs == 0, "With --debug option") or die;
    }
    done_testing();
    exit 0;
  }
  $Carp::MaxArgLen = 0;
  $Carp::MaxArgNums = 25;

  $Spreadsheet::Edit::silent  = $silent  if defined $silent;
  $Spreadsheet::Edit::verbose = $verbose if defined $verbose;
  $Spreadsheet::Edit::debug   = $debug   if defined $debug;
}

$SIG{__WARN__} = sub { 
  Carp::confess("warning trapped; @_") if $silent; 
  die "bug:$_[0]" if $_[0] =~ "uninitialized value";
  warn @_;
};
#$SIG{__DIE__} = sub{ return unless defined($^S) && $^S==0; confess @_ };

# Run this script with --debug to see debug output!
#
sub dprint(@)   { print(@_)                if $debug };
sub dprintf($@) { printf($_[0],@_[1..$#_]) if $debug };

our ($infile, $testdata);
our ($infile2, $testdata2);
BEGIN {
  select STDERR; $|=1; select STDOUT; $|=1;
  # N.B. &title_info encodes knowledge about this data!
  $infile = "/tmp/input.csv";
  $testdata = <<'EOF' ;
Pre-title-row stuff (this is rowx 0)
"A title  ",Btitle,"  Multi Word Title C",,H,F,Gtitle,Z
A2,B2,C2,D2,E2,F2,G2,H2
A3,B3,C3,D3,E3,F3,G3,H3
A4,B4,C4,D4,E4,F4,G4,H4
A5,B5,C5,D5,E5,F5,G5,H5
A6,B6,C6,D6,E6,F6,G6,H6
EOF
  dprint "> Creating $infile\n";
  { open(my $fh,">$infile") || die $!; print $fh $testdata; close $fh || die "Error writing $infile :$!"; }

  $infile2 = "/tmp/input2.csv";
  $testdata2 = <<'EOF' ;
TitleP,TitleQ,,TitleS,TitleT
P1,Q1,R1,S1,T1,U1
P2,Q2,R2,S2,T2,U2
P3,Q3,R3,S3,T3,U3,V3
P4,Q4,R4,S4,T4,U4
P5,Q5,R5,S5,T5,U5
EOF
  dprint "> Creating $infile2\n";
  { open(my $fh,">$infile2") || die $!; print $fh $testdata2; }
}

use Text::CSV::Spreadsheet qw(let2cx cx2let);
use Guard qw(scope_guard);
sub bug(@) { @_=("BUG ",@_); goto &Carp::confess }
sub outercroak (@) { # shows only outer-scope call
  for my $c (0..5) {
    delete local $SIG{__WARN__};
    warn "caller($c)=",avis(caller($c)),"\n";
  }
  my $c=0; while ((caller($c+1))[3] =~ /main::[a-z]/a) { $c++ }
  $Carp::CarpLevel += $c;
  goto &Carp::croak;
}

sub arrays_eq($$) {
  my ($a,$b) = @_;
  return 0 unless @$a == @$b;
  for(my $i=0; $i <= $#$a; $i++) {
    return 0 unless $a->[$i] eq $b->[$i];
  }
  return 1;
}

##########################################################################
package Other {
  BEGIN {*dprint = *main::dprint; *dprintf = *main::dprintf;}
  sub bug(@) { @_=("BUG ",@_); goto &Carp::confess }
  use Data::Dumper::Interp;
  use Spreadsheet::Edit ':FUNCS', # don't import stdvars
                        '@rows' => { -as, '@myrows' },
                        qw(%colx @crow %crow $num_cols @linenums),
                        ;
#                        ], ['$num_cols', '$mync'],
#                   
#  Spreadsheet::Edit::tie_sheet_vars(['@rows','@myrows'], ['$num_cols', '$mync'],
#                                  '%colx', '@linenums',
#                                 );
  use vars qw(@myrows $mync %colx @linenums);
  use vars '$Gtitle';
  $Gtitle = "non-tied-Gtitle-in-Other";
  new_sheet
            data_source => "Othersheet",
            rows => [ [qw(OtitleA OtitleB OtitleC)],
                      [   999,    000,    26      ],
                      [   314,    159,    26      ],
                      [   777,    888,    999     ],
                    ],
            linenums => [ 1..4 ],
            silent => $silent,
            ;
  use vars qw($Othersheet); $Othersheet = sheet();
  dprint "Othersheet = $Othersheet\n";
  title_rx 0;
  tie_column_vars qw(OtitleA OtitleB);
  use vars qw($OtitleA $OtitleB $OtitleC);
} #package Other
##########################################################################

# still in package main;

sub check_other_package() {
  bug unless $Other::Gtitle eq "non-tied-Gtitle-in-Other";
  package Other;
  bug unless $Gtitle eq "non-tied-Gtitle-in-Other";

  apply { };

  apply_torx { 

    bug unless $Other::OtitleA == 314 
  } [2];
  our $OtitleA;
  apply_torx { bug unless $OtitleA == 314 } [2];
  bug unless $Other::Gtitle eq "non-tied-Gtitle-in-Other";
  bug unless @Other::myrows==4 && $Other::myrows[2]->[1]==159;
}
check_other_package;

tie_column_vars {package=>'Other'}, qw(OtitleA OtitleB); # redundant call is ok
check_other_package;

sub hash_subset($@) {
  my ($hash, @keys) = @_;
  return undef if ! defined $hash;
  return { map { exists($hash->{$_}) ? ($_ => $hash->{$_}) : () } @keys }
}
sub fmtsheet() {
  my $s = sheet();
  return "sheet=undef" if ! defined $s;
  "sheet->".vis(hash_subset($$s, qw(colx rows linenums num_cols current_rx title_rx)))
  #"\nsheet=".Data::Dumper::Interp->new()->Maxdepth(2)->vis($s)
}

sub expect1($$) {
  my ($actual, $expected) = @_;
  if (! defined $expected) {
    return if ! defined $actual;
  } else {
    return if defined($actual) && $actual eq $expected;
  }
  die "Expected ",vis($expected), " but got ", vis($actual),
      " at line ", (caller(0))[2], "\n";
}

# Return value of a cell in the 'current' row, verifying that various access
# methods return the same result (could be undef if the item is not defined).
# RETURNS ($value, undef or "bug description");

sub getcell_bykey($;$) { # title, ABC name, alias, etc.
  my ($key, $err) = @_;
  bug unless defined $rx;  # not in apply?

  my $v = $crow{$key};
  my $vstr = vis( $v );

  # Access using the overloaded hash-deref operator of the sheet object
  # (accesses the 'current' row)
  { my $ovstr = vis( sheet()->{$key} );
    $err //= "\$crow{$key} returned $vstr but sheet()->{$key} returned $ovstr"
      if $vstr ne $ovstr;
  }

  # Access using the overloaded array-deref operator of the sheet object,
  # indexing the resulting Magicrow with the Title key.
  my $magicrow = sheet()->[$rx];
  { my $ovstr = vis( $magicrow->{$key} );
    $err //= "\$crow{$key} returned $vstr but sheet()->[$rx]->{$key} returned $ovstr"
      if $vstr ne $ovstr;
  }

  # Index the Magicrow as an array indexed by cx
  { my $cx = $colx{$key};
    $err //= "%colx does not match sheet()->colx for key $key"
      if u($cx) ne u(sheet()->colx->{$key});
    my $ovstr = vis( defined($cx) ? $magicrow->[$cx] : undef );
    $err //= "\$crow{$key} returned $vstr but sheet()->[$rx]->[$cx] returned $ovstr"
      if $vstr ne $ovstr;
   }

  return ($v, $err);
}
sub getcell_byident($;$) { # access by imported $identifier, and all other ways
  my ($ident, $inerr) = @_;
  my ($v, $err) = getcell_bykey($ident, $inerr);
  my $vstr = vis($v);

  my $id_v = eval "\$$ident";   # undef if not defined, not in apply(), etc.
  my $id_vstr = vis($id_v);
  $err //= "\$$ident returned $id_vstr but other access methods returned $vstr"
    if $vstr ne $id_vstr;

  return ($v, $err);
}

# Given a column *data value* indicator L and current cx, return
# indicators of what keys should be usable.
#   L implies data values in the column, e.g. "C" implies "C2","C3"..."C6"
#   (there is no "C0" or "C1" because row 1 is the title row)
sub title_info($$) {
  my ($L,$cx) = @_;

  # infile:
  #   Original col D has an empty title
  #   Original col E has title 'H' which looks like ABC code
  #   Original col F has title 'F' which looks like ABC code
  #   Original col H has title 'Z' which looks like ABC code
  # infile2:
  #   Original col R has an empty title
  #   Original col U has a missing title
  #   Original col V has a missing title and irregular data
  #
  # Other columns have titles which do not look like ABC codes

  my ($title_L, $ABC_usable, $QT_usable) = ($L,1,1);

  my $ABC = cx2let($cx);
  if ($ABC eq "H") {
    $ABC_usable = ($L eq "E");
  }
  elsif ($ABC eq "F") {
    $ABC_usable = ($L eq "F");
  }
  elsif ($ABC eq "Z") {
    $ABC_usable = ($L eq "H");
  }

  if ($L =~ /^[RDUV]$/) {
    $title_L = "";
  }
  elsif ($L eq 'E') {
    $title_L = "H";
  }
  elsif($L eq 'F') {
    # $title_L = "F";
  }
  elsif($L eq 'H') {
    $title_L = "Z";
    $ABC_usable = ($cx == let2cx("E"));
  }
  elsif($L eq 'Z') { # in case many columns are added...
    $ABC_usable = ($cx == let2cx("H"));
  }

  #TODO test dup titles; when we do, set QT_usable = 0

  if ($title_L eq "") {
    $QT_usable = undef;
  }

  return ($title_L, $ABC_usable, $QT_usable);
}

sub check_currow_data($) {
  my $letters = shift;  # specifies order of columns. "*" means don't check
  confess dvis 'WRONG #COLUMNS @letters @crow $rx'
    if length($letters) != @crow;
  die "\$rx not right" unless ${ sheet() }->{current_rx} == $rx;

  for (my $cx=0, my $ABC="A"; $cx < length($letters); $cx++, $ABC++) {
    my $L = substr($letters,$cx,1);

    my ($ABC_v, $err) = getcell_byident($ABC);
    if ($@) { $err //= "ABC $ABC aborts ($@)" }
    elsif (! defined $ABC_v) { $err //= "ABC $ABC is undef" }

    # Manually locate the cell
    my $man_v = $crow[$cx];

    # The Titles    H, F, and Z mask the same-named ABC codes, and refer to
    # orig. columns E, F, and H .
    if ($L ne "*") { # data not irregular
      my $exp_v = "$L$rx"; # expected data value

      if ($man_v ne $exp_v) {
        $err //= ivis 'WRONG DATA accessed by cx: Expecting $exp_v, got $man_v';
      }

      if (defined $title_row) { # Access the cell by title
        # Titles are always valid [with new implementation...]
        my $title = $title_row->[$cx];
        my ($title_L, $ABC_usable, $QT_usable) = title_info($L, $cx);
        if ($title_L ne "") {
          $err //= dvis('$title_L is TRUE but $title is EMPTY')
            if $title eq "";
          (my $vt, $err) = getcell_bykey($title, $err);
          if (u($vt) ne $exp_v) {
            $err //= ivis('row{Title=$title} yields $vt but expecting $exp_v')
          }
        }
        if ($QT_usable) {
          die "bug" if $title_L eq "";
          my $qtitle = "'$title'";
          (my $vqt, $err) = getcell_bykey($qtitle, $err);
          if (u($vqt) ne $exp_v) {
            $err //= ivis('row{QT=$qtitle} yields $vt expecting $exp_v')
          }
        }
      }
    }

    if (defined $err) {
      confess "BUG DETECTED...\n", fmtsheet(), "\n",
              #dvis('$rx $letters $cx $ABC $L $man_v $crow[$cx]\n@crow\n'),
              $err;
    }
  }
  check_other_package();
}
sub check_titles($) {
  my $letters = shift;  # specifies current column order; implies *data* values
  confess "bug" unless length($letters) == $num_cols;
  confess "UNDEF title_row!\n".fmtsheet() unless defined $title_row;
  for (my $cx=0; $cx < length($letters); $cx++) {
    my $L = substr($letters, $cx, 1);
    my ($title_L, $ABC_usable, $QT_usable) = title_info($L, $cx);
    # $title_L is a letter which must appear in the title
    #   or "" if the title should be empty
    # $ABC_usable means the column can be accessed via its ABC letter code.
    # $QT_usable means the column can be accessed via its 'single-quoted Title'
    die "bug" unless $rows[$title_rx]->[$cx] eq $title_row->[$cx];
    my $title = $title_row->[$cx];
    my $qtitle = "'${title}'";
    my $err;
    if ($title_L eq "") {
      $err //= ivis 'SHOULD HAVE EMPTY TITLE, not $title'
        unless $title eq "";
    } else {
      if ($title !~ /\Q$title_L\E/) {
        $err //= ivis 'WRONG TITLE $title (expecting $title_L)'
      }
confess dvis 'BUG1' if ! defined $title;
confess dvis 'BUG2' if ! %colx;
confess "BUG3 title=",vis($title)," colx=",hvis(%colx) if ! defined($colx{$title});
      if ($colx{$title} != $cx) {
        $err //= ivis 'colx{$title} wrong (expecting $cx)'
      }
    }
    apply_torx {
      if ($crow[$cx] ne $title) {
        $err //= ivis 'apply_torx title_rx : row->[$cx] is WRONG';
      }
    } $title_rx;
    apply_torx {
      if ($crow[$cx] ne $title) {
        $err //= ivis 'apply_torx [title_rx] : row->[$cx] is WRONG';
      }
    } [$title_rx];
    if ($ABC_usable) {
      my $ABC = cx2let($cx);
      my $v = $colx{$ABC};
      $err //= ivis('WRONG colx{ABC=$ABC} : Got $v, expecting $cx')
        unless u($v) eq $cx;
    }
    if ($QT_usable) {
      my $v = $colx{$qtitle};
      $err //= ivis('WRONG colx{QT=$qtitle} : Got $v, expecting $cx')
        unless u($v) eq $cx;
    } else {
      # single-quoted title should always work unless there is no valid title
      confess dvis 'test bug: QT_usable should be true for $cx $title_rx $title_row'
        unless ($title_row->[$cx] !~ /\S/
                 or
                grep {$title_row->[$_] eq $title_row->[$cx]} 
                     (0..($cx-1), ($cx+1)..$#$title_row)
               );
    }
    if (defined $err) {
      confess $err, dvis('\n$L $cx $title_L\n'), fmtsheet();
    }
  }
  check_other_package();
}#check_titles

sub check_both($) {
confess "bug" unless defined $title_rx; ###TEMP
  my $letters = shift;  # indicates current column ordering

  my $saved_options = options(verbose => 0);
  scope_guard { options(verbose => $saved_options) };

  croak "Expected $num_cols columns" unless length($letters) == $num_cols;

  check_titles $letters;

  apply {
    # FIXME shouldn't rx *always* be >= title_rx ???
      die "buggy?" if $rx <= $title_rx; 
    return if $rx < $title_rx;  # omit header rows
    check_currow_data($letters)
  };
}

sub verif_eval_err($) {
  my ($ln) = @_;
  my $fn = __FILE__;
  croak "expected error did not occur at line $ln\n" unless $@;

  if ($@ !~ / at $fn line $ln\.?(?:$|\n)/s) {
    croak "Got UN-expected err (did not point to file $fn line $ln): $@\n";
  } else {
    verif_no_internals_mentioned($@);
    dprint "Got expected err: $@\n";
  }
}

# Verify that a column title, alias, etc. is NOT defined
sub check_colspec_is_undef(@) {
  foreach(@_) {
    bug "Colspec ".vis($_)." is unexpectedly defined" 
      if defined $colx{$_};
    eval{ sheet()->colspectocx($_) };
    bug "colspectocx(".vis($_).") unexpectedly did not throw"
      unless $@;
  }
}

# Verify %colx entries, e.g. aliases.  Arguments are any mixture of
# [ $Ident, $CxorABC] or "Ident_CxorABC".
sub check_colx(@) {
  my $colx = sheet()->colx;
  foreach (@_) {
    my ($ident, $cx_or_abc);
    if (ref) {
      ($ident, $cx_or_abc) = @$_
    } else {
      ($ident, $cx_or_abc) = (/^(\w+)_(.*)$/);
    }
    my $cx = ($cx_or_abc =~ /\d/ ? $cx_or_abc : let2cx($cx_or_abc));
    my $actual_cx = $colx->{$ident};
    #outercroak ...
    confess "colx{$ident}=",vis($actual_cx),", expecting $cx (arg=",vis($_),")\n"
      unless u($cx) eq u($actual_cx);
    die "bug" unless sheet()->[2]->{$ident} eq cx2let($cx)."2";
  }
}

sub check_no_sheet() {
  my $pkg = caller;
  for (1,2) {
    confess "current sheet unexpected in $pkg"
      if defined eval("do{ package $pkg; sheet() }");
  }
}

####### MAIN ######
  
# Test data_source();
# Test auto-detecting title row when title_rx() is called
package datasource_autodetect1_test {
  use Data::Dumper::Interp;
  use Spreadsheet::Edit ':all';
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug ;
  read_spreadsheet $main::infile;

  { (my $orig_ds = data_source) =~ /\Q$main::infile\E/ or die;
    data_source "New data source";
    data_source eq "New data source" or die;
    (data_source $orig_ds) eq $orig_ds or die;
    data_source eq $orig_ds or die;
  }
  
  # Title row autodetect tests
  my $s = sheet();
  title_rx {last_cx => 2}; # cx 3 has an empty title

  # autodetect upon tie_column_vars ':all'
  title_rx undef;
  die "bug0" if defined $$s->{title_rx};
  tie_column_vars ':all';
  die "bug1".ivis(' $$s') unless defined title_row();
  die "bug2".ivis(' $$s') unless title_rx() == 1;

  # autodetect upon calling title_rx()
  title_rx undef;
  die "bug0" if defined $$s->{title_rx};
  die "bug1".ivis(' $$s') unless defined title_row();
  die "bug2".ivis(' $$s') unless title_rx() == 1;
  
  # autodetect upon reading $title_rx
  title_rx undef;
  die "bug0" if defined $$s->{title_rx};
  die "bug" unless $title_rx == 1;

  # autodetect upon calling title_rx()
  title_rx undef;
  die "bug0" if defined $$s->{title_rx};
  die "bug" unless title_rx() == 1;
  
  # autodetect upon reading $title_row
  title_rx undef;
  die "bug0" if defined $$s->{title_rx};
  die "bug1" unless defined $title_row;
  die "bug2" unless $title_row->{B} eq "Btitle";

  # autodetect upon referencing tied column var
  title_rx undef;
  apply_torx {
    die "bug0" if defined $$s->{title_rx};
    our $Btitle;
    die "bug" unless $Btitle eq "B2";
    die "bug2" unless defined $$s->{title_rx};
  } 2; 
  
  # Verify disabling autodetect
  title_rx {last_cx => 2, enable => 0};
  title_rx undef;
  die "bug0" if defined $$s->{title_rx};
  die "bug1".ivis(' $$s') if defined title_row();
  title_rx {last_cx => 2};  # re-enable title row
  #die "bug1".ivis(' $$s') unless defined title_row();
};

# Test auto-detecting title row when alias() is called
package autodetect_test2 {
  use Spreadsheet::Edit;
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug;
  read_spreadsheet $main::infile;
  title_rx {last_cx => 2}; # cx 3 has an empty title
  die "bug1" unless 1 == alias "_dummy" => "Btitle";
  die "bug2" unless title_rx == 1;
};

# Test auto-detecting title row on magic-row-hash deref
package autodetect_test3 {
  use Spreadsheet::Edit;
  use Data::Dumper::Interp;
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug;
  title_rx {last_cx => 2, enable => 1}; # redundant "enable" ; cx 3 : empty title
  read_spreadsheet $main::infile;
  die "bug1" unless sheet()->[2]->{Btitle} eq "B2";
  die "bug2" unless title_rx == 1;
};

# auto-detect title row on tied variable reference
package autodetect_test4 { 
  use Spreadsheet::Edit qw(:all);
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug;
  our $Btitle;
  tie_column_vars qw(Btitle); # sans $ sigl
  title_rx {last_cx => 2}; # cx 3 has an empty title
  read_spreadsheet $infile;
  apply_torx { die "bug1" unless $Btitle eq "B2" } 2;
  die "bug2" unless title_rx == 1;
}

# RE-auto-detect after manually modifying a title
# Also check changing sheet in another package
package autodetect_test5 { 
  use Spreadsheet::Edit qw(:all);
  use Data::Dumper::Interp;
  use Guard qw(scope_guard);
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug;
  title_rx {last_cx => 2}; # cx 3 has an empty title
  read_spreadsheet $infile;
  my $s = sheet;
 
  my $saved_main_sheet = package_active_sheet("main");
  scope_guard { sheet {package => "main"}, $saved_main_sheet };
  sheet {package => "main"}, $s;

  main::check_both('ABCDEFGH');
  my $tx = title_rx;
  $rows[$tx]{B} = "New Fancy Title";
  eval{ main::check_both('ABCDEFGH') }; main::verif_eval_err(__LINE__);
  title_rx undef;
  die "bug0" unless $rows[$tx]{"New Fancy Title"} eq "New Fancy Title";
}

# Check that tie_column_vars will refuse to tie pre-existing variables 
# when called in BEGIN{ ... }
package Titleclash_test_onelevel {
  use Spreadsheet::Edit qw(:all);
  BEGIN {
    main::check_no_sheet;
    sheet(undef);  
    options silent => $main::silent, verbose => $main::debug;
    our $Btitle;
    eval { tie_column_vars qw($Btitle) }; main::verif_eval_err(__LINE__);
  }
  # Ditto with the explicit "safe" option
  sheet(undef);  
  options silent => $main::silent, verbose => $main::debug;
  our $Btitle;
  eval { tie_column_vars {safe=>1}, qw($Btitle) }; main::verif_eval_err(__LINE__);
}
package Titleclash::test::Multilevel {
  use Spreadsheet::Edit qw(:all);
  BEGIN {
    main::check_no_sheet;
    sheet(undef);  
    options silent => $main::silent, verbose => $main::debug;
    our $Btitle;
    eval { tie_column_vars qw($Btitle) }; main::verif_eval_err(__LINE__);
  }
  # Ditto with the explicit "safe" option
  sheet(undef);  
  options silent => $main::silent, verbose => $main::debug;
  our $Btitle;
  eval { tie_column_vars {safe=>1}, qw($Btitle) }; main::verif_eval_err(__LINE__);
}

# Test successful tie_column_vars ':all' in BEGIN{},
# implicitly importing all valid variables.
# We will use these (not explicitly declared) variables later.
BEGIN {
  check_no_sheet;
  options silent => $silent, verbose => $debug; # auto-create sheet
  read_spreadsheet $infile;

  { my $s=sheet(); dprint dvis('After reading $infile\n   $$s->{rows}\n   $$s->{colx_desc}\n'); }

  # Aliases are ok which do not reference titles (**NO TITLE ROW YET**)
  alias Aalias => '^';
  alias Aalia2 => 0;
  alias Dalias => 'D';
  alias Ealias => 'E';
  alias Falias => 'F';
  alias Falia2 => 5;
  alias Galias => 'G';
  alias Halias => 'H';
  alias Halia2 => '$';

  check_colx qw(Aalias_0 Aalia2_0 Dalias_D Ealias_E Falias_F
                Falia2_F Galias_G Halias_H Halia2_H);

  { # Test column-letter mappings
    my $cxliststr = join " ", spectocx "A", "B", "C", "Aalias";
    die "spectocx (multi-match) wrong result: $cxliststr"
      unless $cxliststr eq "0 1 2 0";
  }

  # This must be in a BEGIN block so that titles will be auto-exported
  title_rx 1;
  
  # Test alias to title defined before being tied
  alias MWTCalia => qr/Multi Word .*C/;
  
  # Auto-tie all columns, plus the variables mentioned which will become
  # valid later (if not tied here in BEGIN{} then the security check fails).
  tie_column_vars qw( :all Ititle Jtitle Ktitle AAAtitle );

  { my $cxliststr = join " ", 
        spectocx qw(Aalias Aalia2 Dalias Ealias Falias),
                     qr/^[A-Z].*title/;
    die "spectocx (multi-match) wrong result: $cxliststr"
      unless $cxliststr eq "0 0 3 4 5 0 1 6";
  }
}# end BEGIN{}
apply_torx {
  die dvis '$MWTCalia is wrong' unless u($MWTCalia) eq "C2";
} 2;

# "H" is now a title for cx 4, so it masks the ABC code "H".
# Pre-existing aliases remain pointing to their original columns.
alias Halia3 => 'H';

#say dvis '##AAA ${sheet()}';
die "Halia3 gave wrong val"  unless sheet()->[2]->{Halia3} eq "E2";
die "Halias stopped working" unless sheet()->[2]->{Halias} eq "H2";
die "Halia2 stopped working" unless sheet()->[2]->{Halias} eq "H2";
die "Falias stopped working" unless sheet()->[2]->{Falias} eq "F2";

# "F" is also a now title for cx 5, but is the same as the ABC code
alias Falia3 => 'F';
die "Falia3 gave wrong val"  unless sheet()->[2]->{Falia3} eq "F2";
die "Falias stopped working" unless sheet()->[2]->{Falias} eq "F2";

# An alias created with a regexp matching titles, after tie_column_vars in BEGIN
BEGIN { alias Halia4 => qr/^H$/; }
my $Halia4cx = spectocx 'Halia4';
die "wrong alias result" unless $Halia4cx == 4;
our $Halia4;
tie_column_vars 'Halia4';
apply_torx {
  die dvis '$Halia4 is wrong' unless u($Halia4) eq "E2";
} 2;

# Create user alias "A" to another column.  This succeeds because
# ABC codes are hidden by user aliases
alias A => 2;
die "alias 'A' gave wrong val" unless sheet()->[2]->{A} eq "C2";
alias Aalia2 => "A";
die "Aalia2 gave wrong val" unless sheet()->[2]->{Aalia2} eq "C2";
unalias 'A';
die "unaliased 'A' is wrong" unless sheet()->[2]->{A} eq "A2";
die "Aalia2 stopped working" unless sheet()->[2]->{Aalia2} eq "C2";

alias A => 'C';
die "alias 'A' gave wrong val" unless sheet()->[2]->{A} eq "C2";

unalias 'A';
die "'A' after unalias gave wrong val" unless sheet()->[2]->{A} eq "A2";
alias Aalia2 => "A";
die "Aalia2 wrong val" unless sheet()->[2]->{Aalia2} eq "A2";

unalias "Aalia2";
die "unaliased 'A' is wrong" unless sheet()->[2]->{A} eq "A2";

# Try to access the now-undefined alias in a magicrow
eval { $_ = sheet()->[2]->{Aalia2} }; verif_eval_err(__LINE__);

# Try to create a new alias "H" to another column.  This FAILS because
# "H" is a Title, and Titles are always valid.
eval { alias H => 0 }; verif_eval_err(__LINE__);

# Be sure no detritus was left behind when exception was thrown
eval { alias H => 0 } && die "expected exception did not occur";

die "Aalias gave wrong val" unless sheet()->[2]->{Aalias} eq "A2";
die "Dalias gave wrong val" unless sheet()->[2]->{Dalias} eq "D2";
die "Ealias gave wrong val" unless sheet()->[2]->{Ealias} eq "E2";
die "Falias gave wrong val" unless sheet()->[2]->{Falias} eq "F2";
die "Falia2 gave wrong val" unless sheet()->[2]->{Falia2} eq "F2";
die "Galias gave wrong val" unless sheet()->[2]->{Galias} eq "G2";
die "Halias gave wrong val" unless sheet()->[2]->{Halias} eq "H2";
die "Halia2 gave wrong val" unless sheet()->[2]->{Halia2} eq "H2";

# Atitle,Btitle,Multi Word Title C,,,F,Gtitle,Z
check_both('ABCDEFGH');

# Reading and writing whole rows
{ my $r2  = [ map{ "${_}2" } "A".."H" ];
  my $r2x = [ map{ "${_}x" } @$r2 ];
  die "bug" unless arrays_eq($rows[2], $r2);
  $rows[2] = $r2x;
  die dvis 'bug $rows[2]\n$r2x' unless arrays_eq($rows[2], $r2x);
  $rows[2] = \@{$r2};
  die dvis 'bug $rows[2]\n$r2x' unless arrays_eq($rows[2], $r2);
}

# Verify error checks
foreach ([f => 0], [flt => 0, f => 1, flt => undef], [lt => $#rows],
        ) 
{
  my @pairs = @$_; 
  my @saved = ($first_data_rx, $last_data_rx, $title_rx);
  scope_guard {
    first_data_rx $saved[0];
    last_data_rx  $saved[1];
    title_rx      $saved[2];
  };
  local ${sheet()}->{autodetect_opts}->{enable} = 0;
  title_rx { enable => 0 };  # disable autodetect
  while (@pairs) {
    my ($key,$val) = @pairs[0,1]; @pairs = @pairs[2..$#pairs];
    if ($key =~ s/f//) {
      first_data_rx $val;
      die 'bug:first_data_rx as getter' unless u(first_data_rx) eq u($val);
    }
    if ($key =~ s/l//) {
      last_data_rx $val;
      die 'bug:last_data_rx as getter' unless u(last_data_rx) eq u($val);
    }
    if ($key =~ s/t//) {
      title_rx $val;
      die 'bug:title_rx as getter' unless u(title_rx) eq u($val);
    }
    die "BUG $key" if $key ne "";

    # rx out of range
    eval { apply_torx {  } [0..$#rows+1]; }; verif_eval_err(__LINE__);
    eval { apply_torx {  } [-1..$#rows]; }; verif_eval_err(__LINE__);
    eval { apply_exceptrx {  } [0..$#rows+1]; }; verif_eval_err(__LINE__);
    eval { apply_exceptrx {  } [-1..$#rows]; }; verif_eval_err(__LINE__);

    # Attempt to modify read-only sheet variables
    eval { $num_cols = 33 }; verif_eval_err(__LINE__);
    eval { $title_rx = 33 }; verif_eval_err(__LINE__);

    # Access apply-related sheet vars outside apply
    eval { my $i = $rx }; verif_eval_err(__LINE__);
    eval { my $i = $crow[0] }; verif_eval_err(__LINE__);
    eval { my $i = $linenum }; verif_eval_err(__LINE__);
    eval { my $i = $crow{A} }; verif_eval_err(__LINE__);
  }
}

# Flavors of apply
    my %visited;
    sub ck_apply(@) {
      my %actual = map{ $_ => 1 } @_;
      my $visited_str = join ",", sort { $a <=> $b } grep{$visited{$_}} keys %visited;
      foreach(@_){ 
        confess "ck_apply:FAILED TO VISIT $_ (visited $visited_str)" unless $visited{$_}; 
      }
      foreach(keys %visited){ 
        confess "ck_apply:WRONGLY VISITED $_" unless $actual{$_}; 
      }
      while (my($rx,$count) = each %visited) {
        confess "ck_apply:MULTIPLE VISITS TO $rx" if $count != 1;
      }
      %visited = ();
    }
    sub ck_applyargs($$) {
      my ($count, $uargs) = @_;
      die "ck_coldata:WRONG ARG COUNT" unless @$uargs == $count;
      return if $rx <= $title_rx;
      my $L = 'A';
      for my $cx (0..$count-1) {
        my $expval = "${L}$rx";
        confess "ck_coldata:WRONG COL rx=$rx cx=$cx exp=$expval act=$uargs->[$cx]"
          unless $expval eq $uargs->[$cx];
        $L++;
      }
    }
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(2..6);

    first_data_rx 3;
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(3..6);
    first_data_rx undef;
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(2..6);

    last_data_rx 4;
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(2..4);
    last_data_rx undef;
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(2..6);

    first_data_rx 0;  # no-op for apply() because <= title_rx
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(2..6);
    last_data_rx 4;
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(2..4);
    apply_all { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(0..6);
    first_data_rx undef;
    last_data_rx undef;
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(2..6);

    last_data_rx 0; # less than title_rx+1
    apply { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply();
    last_data_rx undef;

    apply_all { $visited{$rx}++; ck_applyargs(0,\@_); } ; ck_apply(0..6);
    foreach my $i (0..6) {
      apply_torx { $visited{$rx}++; ck_applyargs(1,\@_); } $i, 0 ; ck_apply($i);
      apply_torx { $visited{$rx}++; ck_applyargs(2,\@_); } [$i],"A title",1 ; ck_apply($i);
      apply_exceptrx { $visited{$rx}++; ck_applyargs(0,\@_); } $i ; ck_apply(0..$i-1,$i+1..6);
      apply_exceptrx { $visited{$rx}++; ck_applyargs(2,\@_); } $i,0,"Btitle" ; ck_apply(0..$i-1,$i+1..6);
      apply_exceptrx { $visited{$rx}++; } [$i] ; ck_apply(0..$i-1,$i+1..6);
    }
    apply_torx { $visited{$rx}++; } [0..6] ; ck_apply(0..6);
    apply_exceptrx { $visited{$rx}++; } [0..6] ; ck_apply();
    apply_exceptrx { $visited{$rx}++; } [0..5] ; ck_apply(6);

# Change title_rx
    title_rx 3;
      bug unless $title_row->[0] eq "A3";
      apply { $visited{$rx}++; } ; ck_apply(4..6);
    title_rx 4;
      bug unless $title_row->[0] eq "A4";
      bug unless $rows[$title_rx]->[1] eq "B4";
      apply { $visited{$rx}++; } ; ck_apply(5..6);
    title_rx undef; #forget_title_rx;
      apply { $visited{$rx}++; } ; ck_apply(0..6);
    title_rx 0;
      apply { $visited{$rx}++; } ; ck_apply(1..6);

    title_rx 1;  # the correct title row
      apply { $visited{$rx}++; } ; ck_apply(2..6);

# Add and drop rows
    insert_rows 3,4;
    delete_rows 3,4,5,6;
    check_both('ABCDEFGH');

    insert_rows 0,3;      # insert 3 rows at the top

    delete_rows 0..2;  # take them back out
    bug if $title_row->[5] ne 'F';
    check_both('ABCDEFGH');

# Append a new column
    our $Ktitle;  # will be tied
    insert_cols '>$', "Ktitle";
    apply {
      $Ktitle = "K$rx";
    };
    check_both('ABCDEFGHK');

# Insert two new columns before that one
    our ($Ititle, $Jtitle); # will be tied
    insert_cols 'Ktitle', qw(Ititle Jtitle);
    apply {
      bug "rx=$rx" if $rx <= $title_rx;
      $Ititle = "I$rx"; $Jtitle = "J$rx";
      bug unless $Ktitle eq "K$rx";
    };
    check_both('ABCDEFGHIJK');

# Swap A <-> K

    move_cols ">K", "A";
    check_both('BCDEFGHIJKA');

    move_cols '0', "Ktitle";
    check_both('KBCDEFGHIJA');

# And back and forth

    move_cols ">10", qw(A);  # 'A' means cx 0, i.e. Ktitle
    check_both("BCDEFGHIJAK");

    move_cols "^", "A title";
    check_both("ABCDEFGHIJK");

    move_cols '>$', "Multi Word Title C";
    check_both("ABDEFGHIJKC");

    move_cols '>B', '$';
    check_both("ABCDEFGHIJK");

# Delete columns

    apply { bug unless $Gtitle eq "G$rx" };
    delete_cols 'G';
    apply { check_colspec_is_undef('Gtitle') };
    check_both('ABCDEFHIJK');

    delete_cols '^', 'Dalias', '$';
    # "H" (the title of original col E) is no longer a valid ABC code
    # because there are now only 5 columns; so the title "H" can now
    # be used normally, e.g. unquoted
    check_both('BCEFHIJ');


# Put them back


    insert_cols '^', "A title  " ; apply { $A_title = "A$rx" };
    check_both('ABCEFHIJ');

    apply_all { return unless $rx==0; $crow[0] = "Restored initial stuff" };

    insert_cols '>C',""; apply { $crow[3] = "D$rx" };
    check_both('ABCDEFHIJ');

    insert_cols '>F', qw(Gtitle); apply { $Gtitle = "G$rx" };
    check_both('ABCDEFGHIJ');
    apply { bug unless $Gtitle eq "G$rx" };

    insert_cols '>$', qw(Ktitle); apply { $Ktitle = "K$rx" };
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };

# only_cols

    only_cols qw(A B C D E F G Z I J K);   # (no-op)
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };

#sub ttt($) {
#  my $tag = shift;
#  say "### $tag ", join(" ", map{vis} @{ $rows[title_rx] }),"\n";
#  #say dvis '  %colx\n'; 
#  my %shown;
#  foreach(qw/A B C D E F G H I J K L Z/) {
#    my $cx = $colx{$_};
#    say "  $_ →  ",u($colx_desc{$_}), 
#         (defined($cx) && !$shown{$cx}++
#            ? (",  rx2[$cx]=",vis($rows[2]->[$cx])) : ()), "\n"
#  };
#  foreach my $mycx (0..10) {
#    my @hits = grep{ $colx{$_} == $mycx } keys %colx;
#    say "  cx $mycx <- ", join(" ", map{vis} @hits), "\n";
#  }
#}
    only_cols qw(K J I Z F E D C A B); # (re-arrange while deleting G)
    check_both('KJIHFEDCAB');
    apply { check_colspec_is_undef('Gtitle') };

    only_cols qw(8 9 7 6 5 4 3 2 1 0); # (un-re-arrange; G still omitted)
    check_both('ABCDEFHIJK');
    apply { check_colspec_is_undef('Gtitle') };

    # Restore col G
    insert_cols '>F', "Gtitle" ; apply { $Gtitle = "G$rx" };
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };


# Reverse

    reverse_cols;
    check_both('KJIHGFEDCBA');
    apply { bug unless $Gtitle eq "G$rx" };

    reverse_cols;
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };

# Rename

    rename_cols "A title" => "AAAtitle";
    bug unless $title_row->[0] eq 'AAAtitle';

    our $AAAtitle;  # will be tied, but not imported at start
    apply { bug unless $AAAtitle eq "A$rx" };
    check_both('ABCDEFGHIJK');

    rename_cols AAAtitle => "A title  ";
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };
    check_other_package();

# switch sheet

    my $sheet1 = sheet();
    my $p = sheet();
    bug unless defined($p) && $p == $sheet1;
    bug unless $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__} == $sheet1;
    check_other_package();

    # replace with no sheet
    $p = sheet(undef);
    bug unless defined($p) && $p == $sheet1;
    bug if defined $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    bug if defined eval { my $x = $num_cols; } ; # expect undef or croak
    bug if defined eval { my $x = $A_title;   } ; # expect undef or croak
    bug if defined $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    $p = sheet();
    bug if defined $p;
    bug if defined $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    bug if defined sheet();
    check_other_package();

    # put back the first sheet
    check_other_package();
    $p = sheet($sheet1);
    bug if defined $p;
    bug unless $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__} == $sheet1;
    apply { bug unless $Gtitle eq "G$rx" };
    check_both('ABCDEFGHIJK');

    # switch to a different sheet
    new_sheet silent => $silent;
    my $sheet2 = $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    read_spreadsheet $infile2;

    bug unless sheet() == $sheet2;
    apply { check_colspec_is_undef('Gtitle') };
    title_rx 0;
    apply { check_currow_data('PQRSTU*'); };
    apply{ our $TitleP; bug if defined $TitleP; };
    # 10/8/2021: tie_column_vars(Regex args) no longer supported!
    #tie_column_vars qr/^Title/;
    tie_column_vars '$TitleP';
    apply { our $TitleP; bug unless $TitleP eq "P$rx"; 
            check_colspec_is_undef('Gtitle');
          };
    apply { check_currow_data('PQRSTU*'); };

    # switch back to original sheet
    $p = sheet($sheet1);
    bug unless $p == $sheet2;
    bug unless $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__} == $sheet1;
    apply { our $TitleP; bug unless $Gtitle eq "G$rx"; 
            check_colspec_is_undef('TitleP');
          };
    check_both('ABCDEFGHIJK');

    # and back and forth
    sheet($sheet2);
    apply { our $TitleP; bug unless $TitleP eq "P$rx";
            check_colspec_is_undef('Gtitle');
          };
    sheet($sheet1);
    apply { our $TitleP; bug unless $Gtitle eq "G$rx"; 
            check_colspec_is_undef('TitleP');
          };

    # Verify that the OO api does not do anything to the "current package"
    sheet(undef);
    { my $obj = Spreadsheet::Edit->new();
      bug if defined $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    }
    bug if defined sheet();
    check_other_package();

    # Test attaching to another package's sheet
    sheet($sheet1);
    { my $tmp;
      our $Gtitle;
      apply_torx { die "bug($Gtitle)" unless $Gtitle eq "G2" } [2];
      sheet( package_active_sheet("Other") );
      apply_torx { 
        die "bug($Gtitle)" if defined eval{ $Gtitle };
      } [2];
      eval { my $i = defined $Gtitle }; verif_eval_err(__LINE__);
      apply_torx { bug unless $Other::OtitleA == 314 } [2];
      bug unless $Other::Gtitle eq "non-tied-Gtitle-in-Other";
      bug unless $num_cols == 3;
      bug unless @rows==4 && $rows[2]->[0]==314;
      bug unless @Other::myrows==4 && $Other::myrows[2]->[1]==159;
      check_other_package();
      sheet(undef);
      bug if defined $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__};
      bug if defined package_active_sheet(__PACKAGE__);
      bug if defined $Spreadsheet::Edit::OO::pkg2currsheet{"".__PACKAGE__};
      bug if defined sheet();
    }

    # Create an empty sheet with defined num_cols, then add new rows
    my $old_num_cols = $sheet1->num_cols;
    my $old_rows = $sheet1->rows;
    my $old_linenums = $sheet1->linenums;
    my $old_num_rows = scalar @$old_rows;
    new_sheet(num_cols => $old_num_cols, silent => $silent);
      bug unless $num_cols == $old_num_cols;
      bug unless @rows == 0;
    insert_rows 0, $old_num_rows;
      bug unless @rows == $old_num_rows;
    foreach (0..$old_num_rows-1) {
      $rows[$_] = $old_rows->[$_];
    }
    title_rx 1;
    check_both('ABCDEFGHIJK');

    # Create a sheet from existing data
    #new_sheet(rows => $old_rows, silent => $silent);
    new_sheet(rows => $old_rows, linenums => $old_linenums);
    title_rx 1;
    check_both('ABCDEFGHIJK');

    # Put back sheet1
    sheet($sheet1);

# User-defined attributes
    { my $hash = attributes;
      expect1(ref($hash), "HASH");
      expect1(scalar(keys %$hash),0);
      attributes->{key1} = "val1";
      expect1(ref($hash), "HASH");
      expect1(scalar(keys %$hash),1);
      expect1($hash->{key1}, "val1");
      expect1(scalar(keys %{ attributes() }),1);
    }

# transpose

    if ($debug) { print "Before transpose:\n"; write_csv *STDOUT }
    transpose;
    die if defined eval('$title_rx');
    if ($debug) { print "After  transpose:\n"; write_csv *STDOUT }

    transpose;
    die if defined eval('$title_rx');
    apply {
      return if $rx < 2;  # omit header rows
      check_currow_data('ABCDEFGHIJK');
    };
    title_rx 1;
    check_both('ABCDEFGHIJK');

#FIXME: Add tests of more error conditions, e.g.
#  magic $cellvar throws if column was deleted (and not if not)

# Get rid of changes

    delete_cols qw(I J K);

# write_csv

    my $outfile = "/tmp/output.csv";
    write_csv "$outfile";

    { local $/ = undef; # slurp
      open(CHK, "<:crlf", $outfile) || die "Could not open $outfile : $!";
      my $finaldata = <CHK>;
      close CHK;
      my $finald = $finaldata;
      my $testd = $testdata;
      $finald =~ s/"(([^"\\]|\\.)*)"/index($1," ") >= 0 ? "\"$1\"" : $1/esg;
      $finald =~ s/^[^\n]*\n//s; # remove pre-header which we changed
      $testd =~ s/^[^\n]*\n//s;  # ditto
      unless ($finald eq $testd) {
        my $badoff;
        for my $off (0 .. max(length($finaldata),length($testdata))) {
          $badoff=$off, last
            if u(substr($finaldata,$off,1)) ne u(substr($testdata,$off,1));
        }
        die dvisq('\nWRONG DATA WRITTEN (diff starts at offset $badoff)\n\n$finald\n---\n $testd\n===\n$finaldata\n---\n $testdata\n').fmtsheet()
      } 
    }
    check_other_package();

# sort
{   package Other;
    dprint "> Running sort_rows test\n";

    sort_rows { 
                my ($p, $q)=@_; 
                Carp::confess("bug1") unless defined($p);
                Carp::confess("bug2") unless $myrows[$p] == $a;
                Carp::confess("bug3") unless $myrows[$q] == $b;
my $r = $myrows[$p]->[$colx{OtitleA}] <=> $myrows[$q]->[$colx{OtitleA}];
                $myrows[$p]->[$colx{OtitleA}] <=> $myrows[$q]->[$colx{OtitleA}] 
              };
    die "rows wrong after sort\n",vis([map{$_->[0]} @myrows])
      unless main::arrays_eq [map{$_->[0]} @myrows], ["OtitleA",314,777,999];
    die dvis 'linenums wrong after sort: @linenums'
      unless main::arrays_eq \@linenums, [1,3,4,2];
    my @Bs;
    apply { push @Bs, $OtitleB };
    die "apply broken after sort" unless main::arrays_eq \@Bs, [159, 888, 000];
}

exit 0;

