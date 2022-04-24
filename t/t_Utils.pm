package t_Utils;

use t_Setup;
use parent "Exporter::Tiny";

use Spreadsheet::Edit qw(sheet);
use Carp;

our @EXPORT = qw( dprint dprintf
                  verif_no_internals_mentioned bug arrays_eq
                  hash_subset fmtsheet
                  expect1 verif_eval_err check_colspec_is_undef
                  check_no_sheet 
                  create_testdata
                  write_string_to_tmpf
                );

sub dprint(@)   { print(@_)                if $debug };
sub dprintf($@) { printf($_[0],@_[1..$#_]) if $debug };

sub verif_no_internals_mentioned($) {
  my $original = shift;
  return if $Carp::Verbose; 

  local $_ = $original;

  # Ignore glob refs like \*{"..."}
  s/(?<!\\)\\\*\{"[^"]*"\}//g;
  
  # Ignore globs like *main::STDOUT or *main::$f
  s/(?<!\\)\*\w[\w:\$]*\b//g;
  
  # Ignore object refs like Some::Package=THING(hexaddr)
  s/(?<!\w)\w[\w:\$]*=(?:REF|ARRAY|HASH|SCALAR|CODE|GLOB)\(0x[0-9a-f]+\)//g;
  
  # Ignore references to our test library packages, e.g. /path/to/t/t_Utils.pm
  s#\bt_Utils.pm(\W|$)#<ELIDED>$1#gs;
  
  my $msg;
  if (/\b(?<hit>Spreadsheet::[\w:]*)/) {
    $msg = "ERROR: Log msg or traceback mentions internal package '$+{hit}'"
  }
  elsif (/(?<hit>[-.\w\/]+\.pm\b)/s) {
    $msg = "ERROR: Log msg or traceback mentions non-test .pm file '$+{hit}'"
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
  1
}

sub bug(@) { @_=("BUG ",@_); goto &Carp::confess }

sub arrays_eq($$) {
  my ($a,$b) = @_;
  return 0 unless @$a == @$b;
  for(my $i=0; $i <= $#$a; $i++) {
    return 0 unless $a->[$i] eq $b->[$i];
  }
  return 1;
}

sub hash_subset($@) {
  my ($hash, @keys) = @_;
  return undef if ! defined $hash;
  return { map { exists($hash->{$_}) ? ($_ => $hash->{$_}) : () } @keys }
}
sub fmtsheet(;$) {
  my $s = $_[0] // sheet({package => caller});
  return "sheet=undef" if ! defined $s;
  "sheet->".vis($$s)
  #"sheet->".vis(hash_subset($$s, qw(colx rows linenums num_cols current_rx title_rx)))
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

sub verif_eval_err(;$) {  # MUST be called on same line as the 'eval'
  my ($ln) = @_;
  my @caller = caller(0);
  $ln //= $caller[2];
  my $fn = $caller[1];
  confess "expected error did not occur at $fn line $ln\n",
          fmtsheet(sheet({package => $caller[0]}))
    unless $@;

  if ($@ !~ / at $fn line $ln\.?(?:$|\n)/s) {
    confess "Got UN-expected err (not ' at $fn line $ln'):\n«$@»\n\n",
            fmtsheet(sheet({package => $caller[0]})) ;
  } else {
    verif_no_internals_mentioned($@);
    dprint "Got expected err: $@\n";
  }
}

# Verify that a column title, alias, etc. is NOT defined
sub check_colspec_is_undef(@) {
  my $pkg = caller;
  no strict 'refs';
  my $s = sheet({package => caller});
  foreach(@_) {
    bug "Colspec ".vis($_)." is unexpectedly defined" 
      if defined ${"$pkg\::colx"}{$_};
    eval{ $s->spectocx($_) }; verif_eval_err;
  }
}

sub check_no_sheet() {
  my $pkg = caller;
  for (1,2) {
    confess "current sheet unexpected in $pkg"
      if defined eval("do{ package $pkg; sheet() }");
    confess "bug2 $pkg"
      if defined sheet({package => $pkg});
  }
}

# Returns ($testdata, $csvpath)
sub write_string_to_tmpf($$) {
  my ($id, $string) = @_;
  my $tempfile_pfx = $debug ? "/tmp/string" : "/tmp/td_$$";
  my $path = $tempfile_pfx."_${id}.csv";
  dprint "> Creating $path\n";
  die "$path ALREADY EXISTS" if !$debug && -e $path;
  open(my $fh,">", $path) || die $!;
  print $fh $string; 
  close $fh || die "Error writing $path :$!";
  $path
}
sub create_testdata(@) {
  my %args = @_;
  my @rows = @{ $args{rows} // croak "{rows} is required" };
  if ($args{gen_rows}) {
    # Generate extra systematic rows with cell values like "C4"
    my $num_cols = $args{num_cols} // scalar @{ $rows[-1] };
    for my $rx (scalar(@rows) .. scalar(@rows)+$args{gen_rows}-1) {
      push @rows, [ map{ cx2let($_).$rx } 0..$num_cols-1 ];
    }
  }
  my $td = join("", map{ join(",",map{/[\s'",]/ ? quotekey : $_} @$_)."\n" } @rows);
  my $path = write_string_to_tmpf($args{name} // "", $td);
  wantarray ? ($td, $path) : $path
}

1;
