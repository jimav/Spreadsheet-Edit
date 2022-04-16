# License: http://creativecommons.org/publicdomain/zero/1.0/
# (CC0 or Public Domain).  To the extent possible under law, the author, 
# Jim Avera (email jim.avera at gmail dot com) has waived all copyright and 
# related or neighboring rights to this document.  Attribution is requested
# but not required.

# $Id: OO.pm,v 1.153 2022/04/14 14:58:12 jima Exp jima $

# TODO: Need api to *read* options without changing them

# TODO: Add some way to exit an apply() early, e.g. return value?
#       or maybe provide an abort_apply(resultval) function
#       which throws an exception we can catch in apply?

# TODO: Use Tie::File to avoid storing entire sheet in memory
# (requires seekable, so must depend on Text::CSV::Spreadsheet::OpenAsCsv
# copying "-" to a temp file if it is not a plain file).

use strict; use warnings FATAL => 'all'; use feature qw(switch say state);
use utf8;

package Text::CSV::Edit::OO;

# N.B. "our" just creates a *lexical* alias to a package global variable
{ our @CARP_NOT; push @CARP_NOT, qw(Text::CSV::Edit Tie::Indirect::Scalar) }

use utf8 qw/is_utf8/;

use Exporter 'import';
our @EXPORT_OK = qw(%pkg2currsheet); # for use only by Text::Edit::CSV

#our @EXPORT_OK = qw(sheet tie_sheet_vars %pkg2currsheet);

use constant DEFAULT_WRITE_ENCODING => 'UTF-8';
#use constant DEFAULT_READ_ENCODINGS => 'UTF-8,windows-1252';

use Symbol qw(gensym);
use File::Spec qw(catfile);
use File::Temp qw(tempfile tempdir);
use File::Basename qw(basename dirname fileparse);
use Getopt::Long qw(GetOptions GetOptionsFromArray);
use Carp; 
use Text::ParseWords qw(quotewords shellwords);
use Scalar::Util qw(looks_like_number);

use lib "$ENV{HOME}/lib/perl";
use Vis qw(:DEFAULT u);
use Text::CSV::Spreadsheet qw(
   OpenAsCsv @sane_CSV_read_options @sane_CSV_write_options
   cx2let let2cx cxrx2sheetaddr convert_spreadsheet);
use FromCPAN 'Text::CSV';

sub to_array(@)  { @_ > 1 ? @_ 
                   : @_==0 ? ()
                   : ref($_[0]) eq "ARRAY" ? @{$_[0]} 
                   : ref($_[0]) eq "HASH"  ? @{ %{$_[0]} }   # key => value, ...
                   : ($_[0]) 
                 }
sub to_aref(@)   { [ to_array(@_) ] }
sub to_wanted(@) { goto &to_array if wantarray; goto &to_aref }
sub to_hash(@)   { @_==1 && ref($_[0]) eq "HASH" ? $_[0] 
                   : (@_ % 2)!=0 ? croak("odd arg count (@_), expecting key => value pairs")
                   : { to_array(@_) } 
                 }

# %opts = $self->process_args(\@_, "arg1 desc" => \$ar1gvar, "arg2 desc" => \$arg2var ...)
#
#  Check for missing required arguments and form of key => value pairs:
#
#  Copy initial args into the specified fixed-arg variables (via refs),
#  diagnosing undef or missing args using the corresponding description.
#  If additional args remain, check for an even number (key => value pairs)
#  and return them if called in array context, or diagnose extra args if
#  called in scalar or void context.
sub process_args_func { # also called from Text::CSV::Edit::new
  my $aref = shift;
  my $num_fixed_args = @_ / 2;
  my $err;
  for (my $n=0; $n < $num_fixed_args; $n++) {
    my $desc = $_[$n*2];
    $err //= "Missing arg: $desc" unless @$aref > $n;
    my $val = $aref->[$n];
    $err //= "Undef arg: $desc" unless defined $val;
    ${ $_[$n*2+1] } = $val; # store in referenced variable
  }
  $err //= "Extraneous argument(s): @$aref[$num_fixed_args..$#$aref]"
    if @$aref > $num_fixed_args && !wantarray;
  if (@$aref && ref($aref->[-1]) eq "HASH") {
    # Allow final {key => value,...} hash with additional option specs
    $aref = [@$aref[0..$#$aref-1], %{ $aref->[-1] }];
  }
  $err //= "Expecting key => value pairs"
    if ((@$aref-$num_fixed_args) % 2) != 0;
  for (my $i=$num_fixed_args; $i < $#$aref ; $i += 2) {
    if ($aref->[$i] !~ /^\w+$/) {
      $err //= "Suspicious key '".$aref->[$i]."'";
    }
  }
  return ($err, [ @$aref[$num_fixed_args..$#$aref] ]);
}
sub process_args {
  my $self = shift;
  my ($err, $optsaref) = &process_args_func; # \@userargs, "desc"=>\$var, ...
  if ($err) {
    croak $self->logmethmsg(1+1,\": $err");
  }
  confess("bug") if !wantarray && defined(wantarray); # bug if scalar context
  @$optsaref
}

# This global is used by Text::CSV::Edit::logmsg to infer the current sheet
# if an apply is active, even if logmsg is called indirectly via another pkg
our $_inner_apply_sheet;  # see &_apply_to_rows

# Validate a hash of key => value pairs.
# The validator may be an array of valid keynames,
# OR a hash containing pairs of the form
#   key => BOOL | SUBREF
sub check_opthash($$$;@) {
  my ($optshash, $requiredopts, $vhash, @validator_args) = @_;
  $vhash = { map{$_ => 1} @$vhash } if ref($vhash) eq "ARRAY";
  while (my ($k,$v) = each %$optshash) {
    my $validator = $vhash->{$k};
    croak "Unkown option '$k' (valid keys: ".join(" ",keys %$vhash).")"
      unless $validator;
    $validator = $validator->($k,$v,@validator_args)
      if ref($validator) eq 'CODE';
    croak "Invalid value '$v' for option '$k'" unless $validator;
  }
  foreach my $k (@$requiredopts) {
    croak "Option '$k' is required" unless exists $optshash->{$k};
  }
}

# The "current sheet", to which tied globals refer in any given package.
our %pkg2currsheet;

################ GENERAL UTILITIES ################

sub oops(@) { unshift @_, "oops - "; goto &Carp::confess; }

sub min(@) { my $r=shift; foreach(@_) { $r=$_ if ($_ < $r); } $r; }
sub max(@) { my $r=shift; foreach(@_) { $r=$_ if ($_ > $r); } $r; }

sub fmt_colspec_cx($$) {
  my ($colspec, $cx) = @_;
  ref($colspec) eq "Regexp" ? "qr{${colspec}} [cx $cx]"
    : $colspec eq "$cx" ? "cx $cx"
    : "$colspec [cx $cx]"
}
sub fmt_cx($) {
  my ($cx) = @_;
  "cx $cx (".cx2let($cx).")"
}

# Format a list as "(item item ...)" without quotes
sub fmt_uqlist(@) {
  "(".join(" ",map{u} @_).")"
}

# Format a list as key => value pairs without parenthesis
sub fmt_pairs(@) {
  my $result = "";
  while (@_) { 
    confess "Odd arg count, expecting key => value pairs" if @_==1;
    $result .= ", " if $result;
    my $key = shift @_;
    my $val = shift @_;
    $key = visq($key) unless $key =~ /^[[:alpha:]][[:alnum:]_]*$/;
    $result .= "$key => ".vis($val);
  }
  $result
}

# Caller of caller of caller (or specified level's caller)
sub callingsub(;$) {
  my ($levels_back) = @_;
  local $_ = (caller(($levels_back // 1)+1))[3] // oops;
  s/.*:://;
  $_;
}
sub callers_pkg {
  my ($self) = @_;
  my $caller_level = $$self->{caller_level};
  my $pkg = caller($caller_level+1);
  confess "bug" if $pkg =~ /Edit::OO/;
  #Carp::cluck "## callers_pkg got $pkg lcl=$caller_level\n" if $pkg eq 'main';
  $pkg;
}

# N.B. this is exposed to the public via a wriapper in Text::CSV::Edit
sub title2ident(_) { local $_ = shift; s/\W/_/g; s/^(?=\d)/_/; $_ }

# Expect a single {option => value} or list of opt=>val, ...
# Returns hashref containing a copy
# MUST BE INVOKED WITH  &get_args_as_opthash
#   FIXME: Get rid of this and use process_args_func()
sub get_args_as_opthash { 
  return { }            if @_==0;
  return { %{ $_[0] } } if @_==1 && ref($_[0]) eq 'HASH';
  return { @_ }         if (@_ % 2) eq 0;
  croak((caller(1))[3], " expects either {opt=>val,...} hash or list of opt=>val,... \n");
}

# Get an initial _optional_ {option => value, ...} argument, else {}.
# If the hashref arg is present it is shifted off, and a copy is returned.
# MUST BE INVOKED WITH  &shift_optional_initial_hashref  so that the
# caller's @_ can be modified.
sub shift_optional_initial_hashref { 
  return { %{ shift @_ } } if @_ && ref($_[0]) eq 'HASH';
  { }
}
sub check_Nmethargs($$) { # N doesn't include $self
  my ($N,$actual) = @_;
  croak((caller(1))[3], " expects $N arguments!\n") if $actual != ($N+1)
}
sub check_0args { croak((caller(1))[3], " expects no arguments!\n") if @_ != 1}
sub check0_get_current_rx {
  my ($self) = @_;
  croak((caller(1))[3], " expects no arguments!\n") if @_ != 1;
  my $current_rx = $$self->{current_rx};
  confess((caller(1))[3], " is valid only during apply*\n")
    if ! defined $current_rx;
  return $current_rx;
}
#sub valid_only_in_apply($) {
#  # ?? mess with @CARP_NOT to not show Data::Dumper as the source location ??
#  # our @CARP_NOT; push @CARP_NOT, qw(DB Data::Dumper);
#  croak "@_ is not valid except during apply()\n";
#}

sub check_rx {
  my ($self, $rx, $onepastendok) = @_;
  confess callingsub.": Illegal rx ",vis($rx),"\n"
    unless ($rx//"") =~ /^\d+$/;  # non-negative integer
  my $maxrx = $#{$$self->{rows}};
  confess callingsub.": rx ".vis($rx)." is beyond the last row\n"
                    .dvis(' $$self')
    if $rx > ($onepastendok ? ($maxrx+1) : $maxrx);
}

sub checkwantarray($) {
  my $num_results = shift;
  my $wantarray = (caller(1))[5];
  croak((caller(1))[3], " called in scalar context but multiple values would be returned:\n")
    if $num_results > 1 && defined($wantarray) && !$wantarray;
}

##############################################################################

# Provide a "virtual hash" which accesses elements in a specific (constant) 
# row, or else the 'current' row (if rx is undef).
#
# Elements are accessed by key string (e.g. Title, ABC name, alias, etc.)
#
# The 'current row' behavior (with undef rx) is used to implement a
# sheet's "rowhash", which users de-reference during apply().
#
# The specific-rx behavior is used to implement a Magicrow representing
# a specific row.  For example, created on demand when a sheet object
# is de-referenced as an array (see overloads).  

package Text::CSV::Edit::OO::RowhashTie;

require Tie::Hash;
use vars '@ISA'; @ISA = qw(Tie::ExtraHash);  # see Tie::Hash

use Carp; 
{ our @CARP_NOT; push @CARP_NOT, qw(Tie::Indirect::Scalar Tie::Indirect::Hash Text::CSV::Edit::OO) }
sub oops(@) { @_ = ("oops:@_"); goto &Carp::confess; }

sub TIEHASH {
  my ($class, $rx, $sheet_obj) = @_;
  return bless [$$sheet_obj, $rx, undef, $sheet_obj], $class;
}

sub Getrow {
  my $this = $_[0];
  my ($sheet_hash, $rx) = @$this;
  if (! defined $rx) {
    $rx = $sheet_hash->{current_rx};
    if (! defined $rx) {
      # Don't throw exception if being inspected by Data::Dumper
      # or anything which subclasses it (e.g. Vis)
      foreach (0..3) {
        my $pkg = (caller($_))[0];
        # see perldoc UNIVERSAL
        return undef if defined($pkg) && $pkg->isa("Data::Dumper")
      }
      croak "Can't access 'current row' values: Not during apply*\n";
      #confess "Can't access 'current row' values: Not during apply*\n";
    }
  }
  return $sheet_hash->{rows}->[$rx] // oops "bad rx $rx?";
}
sub Cellref {
  my ($this, $key) = @_;
  my $row = &Getrow // return undef;
  my $sheet_hash = $this->[0];
  my $colx = $sheet_hash->{colx};
  # auto-detect title row if appriate
  my $cx = $colx->{$key} // eval{ $this->[3]->_spec2cx($key) }; 
  if (defined $cx) {
    oops if $cx < 0 || $cx > $#$row;
    return \$row->[$cx]; 
  }

  # might be undef for titles hidden due to collisions
  #return undef;
  croak "No such column ",Vis::visq($key)," in ",
    Vis::qsh($sheet_hash->{data_source}//"sheet")
}
sub FETCH {
  my $r = &Cellref;
  return (defined $r ? $$r : undef);
}
sub STORE {
  my ($this, $key, $newval) = @_;
  my $r = &Cellref;
  croak "Attempt to store into undefined column '$key'\n"
    unless defined $r;
  # Arrgh. As of perl v5.30.0 "local $tiedscalar = value" will
  # always first store undef, then store the specified value.
  # If we die here, then tied column variables can't be localized.
  oops "STORE to \"$key\" - attempt to store undef" unless defined $newval;
  ${ $r } = $_[2]; $newval;  #??? is this a Perlbug workaround??
}
sub EXISTS {
  my $r = &Cellref;
  return $r ? 1 : 0;
}
sub FIRSTKEY {  
  # start iterating with 'each'.  We make a copy of the shared colx hash so
  # that ierations can be active simultaneously on multiple instances.
  my $this = shift;
  my $colxcopy = { %{ $this->[0]->{colx} } };
  $this->[2] = $colxcopy;
  return each %$colxcopy;
}
sub NEXTKEY {  
  my $this = shift;
  my $colxcopy = $this->[2] // oops;
  return each %$colxcopy;
}
sub SCALAR   { my $this = shift; scalar %{ $this->[0]->{colx} } }
sub CLEAR { croak "You can't change table geometry this way\n" }
sub DELETE { croak "You can't delete individual cells\n" }

##############################################################################
#
# MagicrowsTiePkg is a factory which represents the set of rows (i.e. all data)
# and creates Magicrow objects on demand to access individual rows.
#
# %magicrows is tied to this package.
#
# An alternate implementation would be to maintain an array of Magicrow
# objects corresponding to {rows} and update the rx values stored inside them
# whenever rows are added or deleted, etc.  That might be more efficient for
# small data sets (less frequent creation and destruction of Magicrow objects),
# but might use too much memory for very large data sets.

package Text::CSV::Edit::OO::MagicrowsTiePkg;
use Carp;

require Tie::Array;
use vars '@ISA'; @ISA = qw(Tie::Array); 

sub TIEARRAY {
  my ($class, $sheet_obj) = @_;
  return bless \$sheet_obj, $class;
}
sub immutable { croak "magicrows is not mutable" }
sub STORESIZE { goto &immutable }
sub STORE     { goto &immutable }
sub DELETE    { goto &immutable }
#sub FETCHSIZE { scalar @{ ${$_[0]}->{rows} } };

sub FETCHSIZE { 
  my $sheetobj = ${$_[0]};
  my $hash = $$sheetobj;
  #while (my ($k,$v) = each %$hash) { $v //= "undef"; warn "###    k=$k v=$v\n"; }
  die "bug1" unless $${$_[0]}->{rows} == $hash->{rows};
  die "bug1" unless @{ $${$_[0]}->{rows} } == @{ $hash->{rows} };
  scalar @{ $${$_[0]}->{rows} } 
};

#sub EXISTS { my $rx = $_[1]; $rx >= 0 && $rx <= $#{ $${$_[0]}->{rows} } }
sub EXISTS {
  my ($this, $rx) = @_;
  my $sheet_obj = $$this;
  my $hash = $$sheet_obj;
  $rx >= 0 && $rx <= $#{ $hash->{rows} }
}
sub FETCH {
  my ($this, $rx) = @_;
  my $sheet_obj = $$this;
  croak "row index $rx out of bounds (only ",$this->FETCHSIZE()," rows exist in ",$$sheet_obj->{data_source},")\n" unless $this->EXISTS($rx);
  # TODO: Cache a small number of these (and invalidate when rows are added/deleted)
  # (after measuring that this is worth the trouble...)
  return Text::CSV::Edit::OO::Magicrow->new($sheet_obj, $rx);
}

##############################################################################
#
# Magicrow objects represent a single row, and behave as either an
# array-ref or hash-ref (both deref operators are overloaded).  
# As an array, cells are accessed by cx.  
# As a hash, cells are accessed by Title, etc. keys.
# FUTURE: An exception is thrown if an undefined column is accessed.
#

package Text::CSV::Edit::OO::Magicrow;

sub new {
  my ($class, $sheet_obj, $rx) = @_;
  my %tiedhash;
  tie %tiedhash, 'Text::CSV::Edit::OO::RowhashTie', $rx, $sheet_obj;
  my $rep = [$sheet_obj, $rx, \%tiedhash];
  # N.B. returning scalar ref to avoid infinite recursion accessing our own rep
  bless \$rep, $class; 
}
sub magicrow_hashderef {
  my $self = shift;
  my (undef, undef, $tiedhash) = @$$self;
  return $tiedhash;
}
sub magicrow_arrayderef {
  my $self = shift;
  my ($sheet_obj, $rx) = @$$self;
  return $$sheet_obj->{rows}->[$rx];
}
use overload 'fallback' => 1,
             '%{}' => \&magicrow_hashderef,
             '@{}' => \&magicrow_arrayderef,
             ;

##############################################################################

package Text::CSV::Edit::OO;
require Tie::Indirect;

{ our @CARP_NOT; push @CARP_NOT, qw/DB Data::Dumper Text::CSV::Spreadsheet/ };

### METHODS ###

# Called after completely replacing sheet content (new, read_spreadsheet, invert)
# Returns the current sheet object
sub _newdata {
  my ($self) = @_;

  my ($rows, $linenums, $num_cols, $current_rx)
    = @$$self{qw/rows linenums num_cols current_rx/};

  croak "Can not switch sheet during an apply!\n"
    if defined $current_rx;
  croak "New 'rows' must be a ref to array of row-refs\n"
    unless ref($rows) eq "ARRAY" 
           && (@$rows==0 || ref($rows->[0]) eq "ARRAY")
           && (@$rows==0 || @{$rows->[0]}==0 || ref($rows->[0]->[0]) eq "");
  croak 'New \'linenums\' must be a ref to array of numbers, "", "?"s, ',
        ' or undefs',svis('\nnot $linenums\n')
    unless ref($linenums) eq "ARRAY" 
           && (@$linenums==0 || !defined($linenums->[0]) 
               || looks_like_number($linenums->[0])
               || $linenums->[0] eq "" 
               || $linenums->[0] =~ /^\?+$/);

  if (@$rows) {
    my $numc = 0;
    foreach my $row (@$rows) { $numc = @$row if @$row > $numc }
#    while(my ($x, $row) = each @$rows) { 
#      if (@$row > $numc) {
#        $numc = @$row;
#        warn "### numc=$numc in rowx $x\n";
#      }
#    }
    if (defined($num_cols) && $num_cols != $numc) {
      croak "num_cols=$num_cols was specified along with initial data,\n",
            "but the value doesn't match the data (which has $numc columns)\n"
    } else {
      $$self->{num_cols} = $num_cols = $numc;
    }
    # Pad short rows with empty fields
    foreach my $row (@$rows) {
      push @$row, (("") x ($num_cols - @$row));
    }
    @$linenums = ("") x @$rows if @$linenums == 0;
  } else {
    # There is no data. Default num_cols to zero, but leave any
    # user-supplied value so a subsequent new_rows() will know how
    # many columns to create.
    $$self->{num_cols} //= 0;
  }
  oops unless $$self->{data_source};
  croak dvisq '#linenums != #rows\n$$self->{linenums}\n$$self->{rows}'
    unless @$linenums == @$rows;

  $$self->{title_rx} = undef;
  $$self->{first_data_rx} = undef;
  $$self->{last_data_rx} = undef;
  $$self->{useraliases} = {};
  $self->rebuild_colx(); # Set up colx colx_desc
  return $self;
}

# Optionally specify the initial data content:
#    clone => existing_sheet
#  OR
#    rows => [rowref, rowref, ...]
#    linenums => [number, number, ...]
#    data_source => "where this data came from"
#  OR
#    num_cols => number # only if data is initially empty
#
#  caller_level => wrapper depth if called internally
#
sub new {
  my $this = shift;
  my $opts = &get_args_as_opthash;
  my $class = ref($this) || $this;

  if (my $clonee = delete $opts->{clone}) { # untested as of 2/12/14
    require Clone;
    return Clone::clone($this); # in all its glory
#    foreach my $key (qw/rows linenums meta_info num_cols/) {
#      croak "'$key' may not be specified when 'clone' is used\n"
#        if defined $opts->{$key};
#      $opts->{$key} = Clone::clone( $$clonee->{$key} );
#    }
#    $opts->{data_source} //= "cloned from $$clonee->{data_source}";
#    foreach my $key (qw/verbose debug silent caller_level iolayers/) {
#      $opts->{$key} //= Clone::clone($$clonee->{$key}) 
#        if defined $$clonee->{$key};
#    }
  }

  my $hash = {
      verbose          => $opts->{verbose} // $opts->{debug} // undef,
      debug            => $opts->{debug} // undef,
      silent           => $opts->{silent} // undef,
      rows             => $opts->{rows} // [],
      linenums         => $opts->{linenums} // [],
      meta_info        => $opts->{meta_info} // [],
      data_source      => $opts->{data_source} // "(none)",
      num_cols         => $opts->{num_cols} // undef,
      caller_level     => 0,
      cmd_nesting      => 0,

      # %colx maps titles, aliases (automatic and user-defined), and
      # spreadsheet column lettercodes to the corresponding column indicies.
      colx             => {},     #
      useraliases      => {},     # key exists for user-defined alias names
      colx_desc        => {},     # for use in error messages

      title_rx         => undef,
      first_data_rx    => undef,
      last_data_rx     => undef,

      # %rowhash maps titles, aliases etc. to an lvalue to the cell
      # in the current row during apply()
      rowhash          => {},
      current_rx       => undef,  # valid during apply()

      # FUTURE: @magicrows is a virtual array of Magicrow (each of which
      # allows associative indexing to cells by any COLSPEC).
      magicrows        => [],
      
      pkg2tiedvarnames => {},
      pkg2tieall       => {},

      attributes       => {},
  };

  # We can't return a hashref directly because the overloaded hash-dereference
  # operator would prevent access (infinite recursion would occur).
  # Instead, the object is a ref to the hashref (i.e. a scalar ref).
  my $self = bless \$hash, $class;

  # Log the call if verbose.
  # Special handling of {caller_level} is needed since there was no object to
  # begin with; instead, internal callers (e.g. Text::CSV::Edit::new) set
  # caller_level as a "user" option.  Here we delete it (so it won't be logged)
  # and temporarily set $$self->{caller_level} accordingly; it will be
  # restored to zero when control returns.
  local $hash->{caller_level} = delete($opts->{caller_level}) // 0;

  # Make the @magicrows be a virtual array of Magicrow for all rows,
  # i.e. Magicrow objects are created on demand (when subsequently
  # used as a hash, a Magicrow references the cells in it's specific row).
  # The sheet object, used as an ARRAY, accesses this via @{} overload.
  tie @{ $hash->{magicrows} }, __PACKAGE__.'::MagicrowsTiePkg', $self;
  
  # Make the %rowhash be a virtual hash mapping keys to cells in current row.
  # The sheet object, used as a HASH, accesses this via %{} overload.
  tie %{ $hash->{rowhash} }, 'Text::CSV::Edit::OO::RowhashTie', undef, $self;

  $self->logmethifv( \(%$opts ? Vis->new()->Maxdepth(1)->hvis(%$opts) : ()), 
                     \" →  ", \"$self");
  $self->_newdata;

  ##$self->subcommand_noverbose("bind_tied_vars");

  return $self;
}

use overload 'fallback' => 1,
             '@{}' => sub {
                        my $self = shift;
                        return $$self->{magicrows};
                      },
             '%{}' => sub {
                        my $self = shift;
                        return $$self->{rowhash};
                      },
             #'""'  => sub { $_[0] },
             #'cmp' => sub { "".$_[0] cmp "".$_[1] },
             ;

sub carponce { # if not silent
  my $self = shift;
  my $msg = join "",@_;
  return if $$self->{carponce}->{$msg}++;
  carp($msg) 
    unless $$self->{silent}; # never appears even if silent is later unset
}

# Allow user to find out names of tied variables
sub tied_varnames {
  my $self = shift;
  my $opts = &shift_optional_initial_hashref;
  my $pkg = $opts->{package} // $self->callers_pkg;
  # ???
  $self->_autodetect_title_rx_ifneeded_cl1() if !defined $$self->{title_rx};
  my $h = $$self->{pkg2tiedvarnames}->{$pkg} //= {};
  return keys %$h;
}

# Internal: Tie specified variables into a package if not already tied.
# Returns:
sub _TCV_REDUNDANT() { 1 }  # if all were already tied
sub _TCV_OK { 2 }           # otherwise (some tied, or no idents specified)
#
sub _tie_col_vars {
  my $self = shift;
  my $pkg  = shift;
  my $safe = shift;  # ignored if undef, otherwise dispositive
  # Remaining arguments are idents

  my ($colx, $colx_desc, $debug, $silent, $useraliases) 
    = @$$self{qw/colx colx_desc debug silent useraliases/};

  my $tiedvarnames = ($$self->{pkg2tiedvarnames}->{$pkg} //= {});
  if (@_ > 0 && %$tiedvarnames) {
    SHORTCUT: {
      foreach (@_) {
        last SHORTCUT unless exists $tiedvarnames->{$_};
      }
      return _TCV_REDUNDANT;
    }
  }

  # Process in cx order for ease of debugging
  VAR: 
  foreach (sort {$a->[0] <=> $b->[0]}
           map {
             my $cx = $colx->{$_};
             defined($cx)
               ? [ $cx,    $_, $colx_desc->{$_} ]
               : [ 999999, $_, "(currently NOT DEFINED)" ];
           } @_
          )
  {
    my ($cx, $ident, $desc) = @$_;

    oops unless $ident =~ /^\w+$/;

    if (exists $tiedvarnames->{$ident}) {
      $self->log(" Previously tied: \$${pkg}::${ident}\n") if $debug;
      next
    }
    oops if $tiedvarnames->{$ident};
    $tiedvarnames->{$ident} = 1;

    $self->log("tie \$${pkg}::${ident} to $desc\n") if $debug;

    # Import a new, anonymous symbol and then tie the imported variable
    no strict 'refs';
    # User declarations (e.g. "our $FOO") will create symbol table entries
    #  so we can't insist that the symtab entry not already exist
    #  in regular run-time situations.
    # However if called during BEGIN{...} we can import symbols to the user's
    #  package so they don't need to be declared explicitly; in that case
    #  we check that malicious titles won't make us clobber user variables
    #  (or any special Perl variables in package main).
    #  The "safe" option also forces (or disables) this behavior.
    if (defined($safe) ? $safe : (${^GLOBAL_PHASE} eq "START")){
      foreach my $p ($pkg, "main") {
        # Per 'man perlref' we can not use *foo{SCALAR} to detect a never-
        # declared SCALAR (it's indistinguishable from an existing undef var).
        # So we must insist that the entire glob does not exist.
        if (exists ${$p.'::'}{$ident}) {
          if (@_ || exists $useraliases->{$ident}) {
            # Die if the user explicitly specified a pre-existing name
            croak "'$ident' clashes with an existing variable in package $p\n",
                  "(Note: This check occurs when tie_column_vars is called in BEGIN{}; tied variables will be imported and you must not explicitly declare them.)\n";
          } else {
            # Assume the user will not need the problematic title, so continue
            carp "WARNING: '$ident' clashes with an existing variable in package $p, so will not be tied for column access\n"
              unless $silent;
          }
          next VAR;
        }
      }
    }
    *{"${pkg}::$ident"} = \${ *{gensym()} };

    # The 'row' sheet member hash is itself tied to code which accesses
    # a cell in the current row, and croaks if not appropriate.  Use it.

    my $to = tie ${"${pkg}::$ident"}, 'Tie::Indirect::Scalar',
                 \&_tiecell_helper, $pkg, $ident;
  }
  return _TCV_OK;
}
sub _tiecell_helper {
  my($mutating, $pkg, $ident) = @_;
  my $sheet = $pkg2currsheet{$pkg};
  confess "No sheet is currently valid for package $pkg\n" 
    unless defined $sheet;
  my $rowhash = $$sheet->{rowhash}; # croaks if not during apply
  croak "Attempt to access currently-undefined column key '$ident'"
    unless exists $rowhash->{$ident};
  \$rowhash->{$ident}
}

sub all_valid_idents {
  my $self = shift;
  my ($title_rx, $rows, $num_cols, $useraliases) 
    = @$$self{qw/title_rx rows num_cols useraliases/};
  my %valid_idents = %$useraliases;
  if (defined $title_rx) {
    foreach (@{ $rows->[$title_rx] }) {
      next unless /\S/;
      $valid_idents{ title2ident($_) } = 1;
    }
  }
  foreach my $cx ( 0..$num_cols-1 ) {
    $valid_idents{ cx2let($cx) } = 1;
  }
  return keys %valid_idents;
}

# {option=>value...} may be passed as the first argument
sub tie_column_vars {
  my $self = shift;
  my $opts = &shift_optional_initial_hashref;
  # Any remaining args specify variable names matching
  # alias names, either user-defined or automatic.
  
  local $$self->{silent}  = $opts->{silent} // $$self->{silent};
  local $$self->{verbose} = $opts->{verbose} // $$self->{verbose};
  local $$self->{debug}   = $opts->{debug} // $$self->{debug};

  my ($title_rx, $rows, $debug, $num_cols, $useraliases) 
    = @$$self{qw/title_rx rows debug num_cols useraliases/};

  my $pkg = $opts->{package} // $self->callers_pkg;

  my @varnames = @_;  # copy
  foreach (@varnames) {
    croak "Invalid variable name '$_'\n" unless /^\$?\w+$/;
    s/^\$//;
  }

  $self->logmethifv(\fmt_uqlist(@varnames), \" in package $pkg");

  if (@varnames == 0) {
    # Tie all possible variables, now and in the future
    $$self->{pkg2tieall}->{$pkg} = 1;
    @varnames = $self->all_valid_idents;
  }

  my $r = $self->_tie_col_vars($pkg, $opts->{safe}, @varnames);
  if ($r == _TCV_REDUNDANT) {
    $self->logmethifv(\"[ALL REDUNDANT] ", \fmt_uqlist(@varnames), 
                      \" in package $pkg");
  }
}

#sub _untie_specified_column_vars {
#  my ($self, @varnames) = @_;
#  return
#    unless @varnames;
#
#  my $pkg = $self->callers_pkg;
#
#  my $tiedvarnames = $$self->{pkg2tiedvarnames}->{$pkg};
#  croak "No column vars are currently tied in package $pkg\n".dvisq('$pkg $tiedvarnames\n$self\n ')
#    unless defined $tiedvarnames;
#
#  foreach my $ident (@varnames) {
#    croak "Column variable ${pkg}::$ident is not currently tied\n".dvisq('$pkg $tiedvarnames\n$self\n ')
#      unless exists $tiedvarnames->{$ident};
#    no strict 'refs';
#    untie ${"${pkg}::$ident"};
#    delete $tiedvarnames->{$ident};
#    # Attempt to "unimport" the symbol.  See Symbol::delete_pacakage().
#    # Note that Perl caches global symbols, so this may not be effective.
#    undef *{"main::${pkg}::${ident}"};
#  }
#}
#sub untie_column_vars {
#  # UNTESTED as of 12/17/2012
#  confess "NOT YET ENABLED";
#  my $self = shift;
#  my $opts = &shift_optional_initial_hashref;
#  croak "untie_column_vars: No variables may be specified (all or nothing)\n" 
#    if @_ > 1;
#  my $pkg = $opts->{package} // $self->callers_pkg;
#
#  my ($pkg2tiedvarnames,$pkg2tieall) = @$$self{qw/pkg2tiedvarnames pkg2tieall/};
#
#  my $tiedvarnames = $pkg2tiedvarnames->{$pkg} //= {};
#
#  $self->logmethifv(avis(keys %$tiedvarnames), \" from pkg $pkg");
#
#  local $$self->{caller_level} = $$self->{caller_level} + 1;
#  $self->_untie_specified_column_vars(keys %$tiedvarnames);
#
#  if (defined(my $old = delete $pkg2tiedvarnames->{$pkg})) {
#    oops if %$old;
#  }
#  delete $pkg2tieall->{$pkg};
#}

#
# Accessors for misc. sheet data
#

# tie_sheet_vars(stdvarname or [stdvarname,uservarname] ...)
#
# This is a FUNCTION not a method!
#
# Import and tie global variables to access sheet variables.
# Thereafter, those variables will always refer to the "current" sheet.
#
# Each argument may be either the name of a standard variable name
# (including the $ @ or % sigl), or else [ <varname>, uservar ] to
# import the same thing under a different name.

sub _scal_tiehelper {  # access a scalar sheet variable
  my($mutating, $pkg, $ident, $uvar, $onlyinapply) = @_;
  croak "Modifying $uvar is not allowed\n" if $mutating;
  my $sheet = $pkg2currsheet{$pkg};
  croak "Can not use $uvar: No sheet is currently valid for package $pkg\n"
    unless defined $sheet;
  croak "Can't use $uvar now: Not during apply*\n"
    if $onlyinapply && ! defined $$sheet->{current_rx};
  return \$$sheet->{$ident}; # return ref to the scalar
}
sub _rval_tiehelper { # access a hash or array sheet variable
  my($mutating, $pkg, $ident, $uvar, $onlyinapply) = @_;
  my $sheet = $pkg2currsheet{$pkg};
  croak "Can not use $uvar now: No sheet is currently valid for package $pkg\n"
     unless defined $sheet;
  croak "Can not use $uvar: Not during apply*\n"
    if $onlyinapply && ! defined $$sheet->{current_rx};
  return $$sheet->{$ident}; # return the value, which is itself a ref
}
sub _aryelem_tiehelper { # access an element of an array sheet variable
  my($mutating, $pkg, $x_ident, $array_ident, $uvar, $onlyinapply) = @_;
  # E.g. for $title_row : x_ident="title_rx", array_ident="rows"
  
  croak "Modification of $uvar is not allowed\n" if $mutating;

  # _scal_tiehelper croaks if there is no sheet, etc.
  my $xref = _scal_tiehelper(0, $pkg, $x_ident, $uvar, $onlyinapply) // oops;
  my $x = $$xref;
  return undef unless defined $x;

  my $sheet = $pkg2currsheet{$pkg};
  my $aref = $$sheet->{$array_ident} // oops; # e.g. "rows"

  oops(dvisq '$sheet') if $x > $#$aref;

  return \$aref->[$x]; # return ref to scalar (the element in the array)
}
sub _tie_sheet_vars_to_pkg($$;$) { # N.B. called by Text::CSV::Edit::import()
  my ($pkg, $varspecs, $debug) = @_;
  foreach (@$varspecs) {
    my ($stdname, $username) = @{ ref($_) ? $_ : [$_,$_] };

    my ($sigl, $stdident) = ($stdname =~ /^([\$\@\%])([\w:]+)$/)
      or croak "tie_sheet_vars: '$stdname' has no sigl or is otherwise bad\n";

    my ($usersigl, $userident) = ($username =~ /^([\$\@\%])([\w:]+)$/)
      or croak "tie_sheet_vars: '$username' must start with a sigl character\n";
    $sigl eq $usersigl
      or croak "tie_sheet_vars: '$stdname' and '$username' must be same type\n";

    my $dmsg = "";
    if ($debug) {
      # print STDERR "import & tie sheet var ${sigl}${pkg}::$userident";
      # print STDERR " to $stdident" if $stdident ne $userident;
      $dmsg .= "import & tie sheet var ${sigl}${pkg}::$userident";
      $dmsg .= " to $stdident" if $stdident ne $userident;
    }

    my $globref = gensym();  # create a separate variable for each pkg
    no strict 'refs';
    *{"${pkg}::$userident"} =
        $sigl eq '$' ? \${ *$globref } :
        $sigl eq '@' ? \@{ *$globref } :
        $sigl eq '%' ? \%{ *$globref } :
        $sigl eq '&' ? \&{ *$globref } :
        $sigl eq '*' ?      $globref   :
        do { die "bug($sigl)($userident)" } ;

    my $uvar = "current-sheet-accessor $username";

    my $tobj = do {
      if ($stdname =~ /^\$(?:num_cols|title_rx|first_data_rx|last_data_rx)/) {
        tie ${"${pkg}::$userident"}, 'Tie::Indirect::Scalar', \&_scal_tiehelper,
            $pkg, $stdident, $username;
      }
      elsif ($stdname eq "\$rx") {
        tie ${"${pkg}::$userident"}, 'Tie::Indirect::Scalar', \&_scal_tiehelper,
            $pkg, "current_rx", $uvar, 1
      }
      elsif ($stdname eq "\$row") {
        tie ${"${pkg}::$userident"}, 'Tie::Indirect::Scalar', \&_aryelem_tiehelper,
            $pkg, "current_rx", "rows", $uvar, 1;
      }
      elsif ($stdname eq "\$linenum") {
        tie ${"${pkg}::$userident"}, 'Tie::Indirect::Scalar', \&_aryelem_tiehelper,
            $pkg, "current_rx", "linenums", $uvar, 1;
      }
      elsif ($stdname eq "\%rowhash" or $stdname eq "\%row") {
        tie %{"${pkg}::$userident"}, 'Tie::Indirect::Hash', \&_rval_tiehelper,
            $pkg, "rowhash", $uvar, 1;
      }
      elsif ($stdname eq "\$title_row") {
        tie ${"${pkg}::$userident"}, 'Tie::Indirect::Scalar', \&_aryelem_tiehelper,
            $pkg, "title_rx", "rows", $uvar;
      }
      elsif ($stdname =~ /^\@(?:rows|linenums|meta_info)/) {
        tie @{"${pkg}::$userident"}, 'Tie::Indirect::Array', \&_rval_tiehelper,
            $pkg, $stdident, $uvar;
      }
      elsif ($stdname =~ /^\%(?:colx|colx_desc)/) {
        tie %{"${pkg}::$userident"}, 'Tie::Indirect::Hash', \&_rval_tiehelper,
            $pkg, $stdident, $uvar;
      }
      else { croak "Unrecognized sheet varname '$_'\n" }
    };
    #print STDERR " tobj=$tobj\n" if $debug;
    #print STDERR "\n" if $debug;
    # $dmsg .= " tobj=$tobj\n" if $debug;
    carp $dmsg if $debug;
  }
}
sub tie_sheet_vars(@) {
  my $opts = &shift_optional_initial_hashref;
  my $pkg = $opts->{package} // caller;
  #my $debug = 1;  # there's no object, so no way to specify this
  my $debug = 0;  # there's no object, so no way to specify this
  _tie_sheet_vars_to_pkg($pkg, \@_, $debug);
}

sub userdata {
  my $self = shift;
  if (@_) {
    $self->{userdata} = $_[0];
  } else {
    return $self->{userdata};
  }
}

sub attributes { &check_0args; ${$_[0]}->{attributes} }
sub colx { &check_0args; ${$_[0]}->{colx} }
sub colx_desc { &check_0args; ${$_[0]}->{colx_desc} }
sub data_source {
  my $self = shift;
  return $$self->{data_source} if @_ == 0;  # 'get' request
  $self->logmethifv(@_);
  croak "Too many args" unless @_ == 1;
  $$self->{data_source} = $_[0]
}
sub iolayers { &check_0args; ${$_[0]}->{iolayers} }
sub linenums { &check_0args; ${$_[0]}->{linenums} }
sub meta_info { &check_0args; ${$_[0]}->{meta_info} }
sub num_cols { &check_0args; ${$_[0]}->{num_cols} }
sub rows { &check_0args; ${$_[0]}->{rows} }
sub sheetname { &check_0args; ${$_[0]}->{sheetname} }
sub input_encoding {
  # Emulate old API.  We actually store input_iolayers instead not,
  # so as to include :crlf if necessary.
  &check_0args;
  local $_;
  return undef unless
    exists(${$_[0]}->{input_iolayers})
    && ${$_[0]}->{input_iolayers} =~ /encoding\(([^()]*)\)/;
  return $1;
}
# See below for title_rx()
sub title_row {
  &check_0args;
  my ($title_rx, $rows) = @${$_[0]}{qw/title_rx rows/};
  defined($title_rx) ? $rows->[$title_rx] : undef
}
sub rx { &check0_get_current_rx; }
sub current_row {
  my $current_rx = &check0_get_current_rx;
  ${$_[0]}->{rows}->[$current_rx];
}
sub linenum {
  my $current_rx = &check0_get_current_rx;
  ${$_[0]}->{linenums}->[$current_rx];
}
sub rowhash { # \%rowhash (or it's synonym %row)
  # Don't check for being in apply now; will be checked at access time
  #&check0_get_current_rx;
  ${$_[0]}->{rowhash};
}
sub _getref {
  check_Nmethargs(2,@_);
  my ($self, $rx, $ident) = @_;
  my ($rows, $colx) = @$$self{qw/rows colx/};
  croak "get/seg: rx $rx is out of range" if $rx < 0 || $rx > $#$rows;
  my $row = $$self->{rows}->[$rx];
  my $cx = $colx->{$ident};
  oops("Invalid cx ".vis($cx)) if ! defined($cx) || $cx < 0 || $cx > $#$row;
  \$row->[$cx];
}
sub get {
  my $ref = &_getref;
  $$ref;
}
sub set {
  my $ref = &_getref;
  $$ref = $_[3];
}

## Call another method, incrementing caller_level
#sub submethod {
#  my $self = shift;
#  my $methname = shift;
#  local $$self->{caller_level} = $$self->{caller_level} + 2;
#  $self->$methname(@_);
#}
#sub subcommand {
#  my $self = shift;
#  my $methname = shift;
#  local $$self->{caller_level} = $$self->{caller_level} + 2;
#  local $$self->{cmd_nesting} = $$self->{cmd_nesting} + 1;
#  $self->$methname(@_);
#}
## Call another method, incrementing caller_level and suppressing verbose
#sub subcommand_noverbose {
#  my $self = shift;
#  my $methname = shift;
#  local $$self->{verbose} = $$self->{debug}; # keep showing with 'debug'
#  local $$self->{caller_level} = $$self->{caller_level} + 2;
#  local $$self->{cmd_nesting} = $$self->{cmd_nesting} + 1;
#  $self->$methname(@_);
#}

# Print segmented log messages:
#   Join args together, prefixing with "> " or ">> " etc.  
#   unless the previous call did not end with newline.
# Maintains internal state.  A final call with an ending \n must occur.
sub log {
  my $self = shift;
  state $in_midst;
  print STDERR join "", 
                    ($in_midst ? "" : (">" x ($$self->{cmd_nesting}||1))), 
                    map{u} @_;
  $in_midst = ($_[$#_] !~ /\n\z/s);
}

# FUNCTION.
# Format a list omitting enclosing brackets, with special annotation handling.
#
# Items are formatted using vis() and separated by commas, except that refs to 
# printable strings are de-referenced and included without other formatting and 
# adjacent commas are suppressed (this special form is used to intermingle 
# fixed annotations among data items).
#
# If the arguments constitute a sequence, then "first..last" is returned
# instead of "arg0,arg1,...,argN" (annotations aren't recognized in this case).
#
# In array context, returns (list of formatted items).
#
sub _is_annotation($) { ref($_[0]) eq 'SCALAR' && ${$_[0]} !~ /[^[:print:]]/ }
sub fmt_list(@) {
  my $is_sequential = (@_ >= 4);
  my $seq;
  foreach(@_) { 
    $is_sequential=0,last 
      unless defined($_) && /^\w+$/ && ($seq//=$_[0])++ eq $_ 
  }
  if ($is_sequential) {
    return wantarray ? (visq($_[0]),"..",vis($_[$#_])) # not so useful
                     : visq($_[0])."..".vis($_[$#_])
  }
  return (map{vis} @_) if wantarray;
  
  # Want a single-string represnetation of a non-sequence:  Join vis()
  # results with commas except for special handling of \"..." annotations
  
  return join "", map{
     _is_annotation($_[$_]) 
       ? ${ $_[$_] } 
       : vis( $_[$_] ) . (($_ < $#_ && !_is_annotation($_[$_+1])) ? "," : "")
                     } (0..$#_)
}
## test
#foreach ([], [1..5], ['f'..'i'], ['a'], ['a','x']) {
#  my @items = @$_;
#  warn avis(@items)," -> ", scalar(fmt_list(@items)), "\n";
#  @items = (\"-FIRST-", @items);
#  warn avis(@items)," -> ", scalar(fmt_list(@items)), "\n";
#  splice @items, int(scalar(@items)/2),0, \"-ANN-" if @items >= 1;
#  warn avis(@items)," -> ", scalar(fmt_list(@items)), "\n";
#  push @items, \"-LAST-";
#  warn avis(@items)," -> ", scalar(fmt_list(@items)), "\n";
#}
#die "TEX";

# This is a FUNCITON not method. 
#
# Format a log message for a sub call using broken-out 
# parameters:  (caller_level, cmd_nesting_level, ITEM...)
# where the ITEMs are formatted with fmt_list, i.e. with annotation support.
#
# Called by related methods and directly in Text::CSV::Edit.
#
# RETURNS "> [callerfile:callerlineno] calledsubname STRINGIFIED,ITEM,...\n"
#   with repeated ">>..." if cmd_nesting_level > 1.
# 
# ITEMs should NOT include a terminal newline.
sub logfunc($$@) {
  my ($caller_level, $nesting, @items) = @_;
  my $msg = fmt_list(@items);
  my (undef,$fn,$ln,$subname) = caller($caller_level+1);  # +1 for calling us
  unless (defined($subname) && defined($fn)) {
    #local $Carp::MaxArgNums = '0 but true'; # omit args from backtrace
    oops dvisq('LOG BUG: $caller_level+1 is off bottom\n  $msg\n');
  } 
  if ($fn =~ /\b((?:Edit|OO)\.pm)/) {
    # If an internal location is shown there is a bug somewhere. 
    # caller_level is normally >= 2, but for debugging we might deliberately 
    # set caller_level=0, in which case we don't diagnose this.
    local $Carp::MaxArgNums = '0 but true'; # omit args from backtrace
    oops "*** LOG BUG ('$1' in '$fn'; ln=$ln cl=$caller_level)\n";
  } 
  $fn = basename($fn);
  $subname =~ s/.*:://;
  #Carp::cluck svis "##LOG $subname cl=$caller_level n=$nesting ${fn}:$ln \$msg\n";
  oops "terminal newline in log arg" if @items && ($items[-1]//"") =~ /\n\z/;
  
  # N.B. fmt_list() handles ref-to-scalar as un-quoted string
  (">" x ($nesting||1))."[$fn:$ln] $subname $msg\n"
}

# $obj->logmethmsg(extra_levels, STRINGs...)
# 
# RETURNS: ">... [callerfile:callerlineno] calledsubname STRINGs\n"
#
# Looks back (caller_level + extra_levels + 1) stack frames 
# (+1 to account for the call to us).
#
# Unconditionally appends \n.
sub logmethmsg {
  oops "missing extra_levels arg" unless @_ > 1 && $_[1] =~ /^\d+$/;
  my $self = shift;
  my $extra_levels = shift;
  logfunc( $$self->{caller_level}+$extra_levels+1,  # *** was +1+1,
           $$self->{cmd_nesting},
           @_ );
}
sub logmeth { 
  my $self = shift; 
  print STDERR $self->logmethmsg(1,@_);
}

sub logmethifv {
  my $self = $_[0]; # not shifted off
  return unless $$self->{verbose};
  goto &logmeth;  # goto so {caller_level} is correct
}

sub _call_usercode($$$) {
  my ($self, $code, $cxlist) = @_;
  local $$self->{caller_level} = 0; # Log user's nested calls correctly

  if (@$cxlist) {
    my $row = $self->current_row();
    foreach ($row->[$cxlist->[0]]) { # bind $_ to the first-specified column
      &$code(@$row[@$cxlist]);
    }
  } else {
    $code->();
    ##Simplify backtraces
    #@_ = ();
    #goto &$code;
  }
}

# Do apply, resetting caller_level to 0 and handling COLSPEC args
# If $rxlists or $rxfirst & $rxlast are undef, visit all rows
#
# Calers must increment {caller_level} first unless they goto.
sub _apply_to_rows($$$;$$$) {
  my ($self, $code, $cxlist, $rxlist, $rxfirst, $rxlast) = @_;
  my $hash = $$self;
  my ($linenums,$rows,$num_cols,$cl) = @$hash{qw/linenums rows num_cols caller_level/};

  croak $self->logmethmsg(0, " Missing or incorrect {code} argument") unless ref($code) eq "CODE";
  foreach (@$cxlist) {
    if ($_ < 0 || $_ >= $num_cols) { 
      croak $self->logmethmsg(0,"cx $_ is out of range") 
    }
  }

  { # Temp save "current_rx" from an enclosing apply
    local $hash->{current_rx} = undef;

    # Temp update "current apply sheet" for logmsg()
    local $_inner_apply_sheet = $self;
    
    if (defined $rxlist) {
      foreach my $rx (@$rxlist) {
        croak $self->logmethmsg(0," Rx $rx is out of range") 
          if $rx < 0 || $rx > $#$rows;
        $hash->{current_rx} = $rx;
        _call_usercode($self,$code,$cxlist);
      }
    } else {
      # Do not cache $#$rows so user can call new_rows() or delete_rows()
      for (my $rx = $rxfirst // 0; 
           $rx <= $#$rows && (!defined($rxlast) || $rx <= $rxlast); 
           $rx++) 
      {
        $hash->{current_rx} = $rx;
        _call_usercode($self,$code,$cxlist);
        $rx = $hash->{current_rx}; # might have been changed by delete_rows()
      }
    }
  }

  croak "After completing apply, an enclosing apply was resumed, but",
        " current_rx=",$hash->{current_rx}," now points beyond the last row!\n"
    if defined($hash->{current_rx}) && $hash->{current_rx} > $#$rows;
}

# Return a pair of hashes ({title=>cx...}, {qtitle=>cx...}).
# Non-unique titles, or titles consisting of '^' or '$' in the "wrong" 
# posistion, are omitted with a warning the first time.  
# Empty titles are ignored.
# 
# N.B. ({}, {}) is returned if there are no titles.
sub _get_titles {
  my $self = shift;
  my ($title_rx, $rows) = @$$self{qw/title_rx rows/};

  my (%titles, %idents, %qtitles);
  if (defined $title_rx) {
    my $title_row = $rows->[$title_rx];
    while (my ($cx, $title) = each @$title_row) {
      next if $title eq "";
      
      # 'single quoted' titles inherently never conflict with specials
      my $qtitle = "'${title}'";
      if (exists $qtitles{$qtitle}) {
        #$self->carponce(svis 'WARNING: Multiple columns have the same Title $title\n');
        $self->carponce(svis 'WARNING: Title $title is in both cx $qtitles{$qtitle} and $cx, and will not be avilable!\n');
        delete $qtitles{$qtitle};
        delete $titles{$title};
        next
      } else {
        $qtitles{$qtitle} = $cx;
      }

      if (($title eq '^' && $cx != 0)
          or 
          ($title eq '$' && $cx != ($$self->{num_cols}-1)))
      {
        $self->carponce(svis('WARNING: Title $title in cx $cx will not be available because\nit is the same as a special symbol\n'))
      } 
      else {
        $titles{$title} = $cx;
      }
    }
  }

  return \%titles, \%qtitles;
}

# Rebuild %colx and %colx_desc, and tie any required new variables.
#
# User-defined column aliases must already be valid in %colx;
# all other entries are deleted and re-created.
#
# Conflicts are resolved as follows:
#
#   ALWAYS VALID:
#      Special indicators ^ and $
#      Titles, except for "^" and "$" which are ignored with warnings.
#      "'single quoted'" versions of all Titles.
#      User-defined aliases (an exception occurs if there is a conflict)
#
#   VALID IF NOT CONFLICTING WITH ABOVE, OTHERWISE OMITTED:
#       ABC letter-codes
#       Numeric strings giving cx values
#       Automatic aliases
#
# Warning are issued once for resolvable conflicts.
# There _must_ not be any unresolvable conflicts, because a caught exception
# would leave %colx in an invalid state.
sub rebuild_colx() {
  my $self = shift;

  my ($silent, $colx, $colx_desc, $useraliases, $num_cols, $title_rx,
      $rows, $debug, $pkg2tieall)
    = @$$self{qw/silent colx colx_desc useraliases num_cols title_rx
                 rows debug pkg2tieall/};

  my %saved_desc = %$colx_desc;  # make a copy
  %$colx_desc = ();
  my $ocx;
  local $Vis::Maxwidth = 0;

  # save User-defined Aliases from old %colx
  my %useralias;
  foreach my $alias (keys %$useraliases) {
    my $cx = $colx->{$alias};
    next if !defined($cx);  # the referenced column was deleted
    $useralias{$alias} = $cx;
    #$colx_desc->{$alias} = "cx $cx: User-defined alias";
    $colx_desc->{$alias} = $saved_desc{$alias} // die "BUG";
  }
  $colx = undef; #debugging

  # Special symbols ^ and $ are always valid
  my %special = ('^' => 0, '$' => $num_cols-1);
  while (my ($sym, $cx) = each %special) {
    $colx_desc->{$sym} = "cx $cx: reserved special symbol '${sym}'"
  }

  # Actual titles may always be used, unless they are '^' or '$'
  my ($titles, $qtitles) = $self->_get_titles;
  while (my ($title, $cx) = each %$titles) {
    if (defined ($ocx = $useralias{$title})) {
      confess "BUG: Conflicting user alias should have been blocked in \&alias"
        unless $ocx == $cx;
      delete $titles->{$title}; # don't diagnose a conflict below
    } else {
      $colx_desc->{$title} = "cx $cx: Title";
    }
  }
  while (my ($qtitle, $cx) = each %$qtitles) {
    $colx_desc->{$qtitle} = "cx $cx: Title surrounded by 'single-quotes'";
  }
      
  my %abc;
  foreach my $cx ( 0..$num_cols-1 ) {
    my $key = cx2let($cx);
    if (defined($ocx = $useralias{$key})) {
    } 
    elsif (defined($ocx = $titles->{$key})) {
      $self->carponce(svis('Title $key for cx $ocx supercedes the same-named letter-code for cx $cx\n'))
        unless $cx == $ocx || $silent;
    } 
    else {
      $abc{$key} = $cx;
      $colx_desc->{$key} = "cx $cx: Standard letter-code"
    }
  }

  my %cxkeys;
  foreach my $cx ( 0..$num_cols-1 ) {
    if (defined($ocx = $titles->{$cx})) {
      $self->carponce(svis('WARNING: A Title "$cx" is defined for cx $ocx, which may confuse code using numeric column indicies\n'))
        unless $cx == $ocx;
    } else {
      $cxkeys{$cx} = $cx;
      #$colx_desc->{$cx} = "cx $cx as itself";
      $colx_desc->{$cx} = "cx $cx";
    }
  }

  # Install all (except autoaliases), verifying that we avoided conflicts
  $colx = $$self->{colx};
  %$colx = ();
  for my $h (\%abc, \%cxkeys, $titles, \%useralias, $qtitles, \%special) {
    while (my ($key, $cx) = each %$h) { 
      if (exists $colx->{$key}) { # bug detected...
        my $msg = dvisq('missed conflict $key $cx\n');
        foreach (qw(\%abc \%cxkeys $titles \%useralias $qtitles \%special)) {
          my $href = eval $_ // die;
          if ($href->{$key}) { $msg .= "  key is in $_\n" }
        }
        oops $msg
      }
      $colx->{$key} = $cx;
    }
  }

  # Now install non-conflicting autoaliases
  my %autoaliases;
  while (my ($title, $cx) = each %$titles) {
    my $ident = title2ident($title);
    my $other_cx = $colx->{$ident};
    if (defined($other_cx) && $other_cx != $cx) {
      $self->carponce("WARNING: Automatic alias '$ident' for cx $cx will be unavailable",
           " (masked by $colx_desc->{$ident})\n")
    } else {
      $autoaliases{$ident} = $cx;
      $colx->{$ident} = $cx;
      $colx_desc->{$ident} = "cx $cx: Automatic alias for title";
    }
  }
  
  # export and tie newly-defined magic variables to packages which want that
  if (my @pkglist = grep {$pkg2tieall->{$_}} keys %$pkg2tieall) {
    my @idents = $self->all_valid_idents;
    foreach my $pkg (@pkglist) {
      $self->_tie_col_vars($pkg, undef, @idents);
    }
  }
} # rebuild_colx

# Move and/or delete column positions.  The argument is a ref to an array
# containing the old column indicies of current (i.e. surviving) columns,
# or undefs for new columns which did not exist previously.
sub adjust_colx {
  my ($self, $old_colxs) = @_;
  my ($colx, $colx_desc, $debug) = @$$self{qw/colx colx_desc debug/};
  my %old2new = map { my $old = $old_colxs->[$_];
                      defined($old) ? ($old => $_):()
                    } 0..$#$old_colxs;
  while (my($alias, $cx) = each %$colx) {
    next unless defined $cx; # e.g. non-unique title; see rebuild_colx()
    if (defined (my $new_cx = $old2new{$cx})) {
      #warn ">adjusting colx{$alias} : $colx->{$alias} -> $new_cx\n" if $debug;
      $colx->{$alias} = $new_cx;
    } else {
      #warn ">deleting colx{$alias} (was $colx->{$alias})\n" if $debug;
      delete $colx->{$alias};
      delete $colx_desc->{$alias};
    }
  }
}

# Translate list of COLSPECs and/or qr/regex/s to a list of [cx,desc].
# Regexes may match multiple columns.
# THROWS if a spec does not indicate any existing column.
# Auto-detects the title row if appropriate.
sub _specs2cxdesclist {
  my $self = shift;
  my ($colx, $colx_desc) = @$$self{qw/colx colx_desc/};
  my @results;
  foreach my $spec (@_) {
    if (defined (my $cx = $colx->{$spec})) {
      push @results, [$cx, $colx_desc->{$spec}];
      next
    }
    redo if !defined($$self->{title_rx}) 
              && $self->_autodetect_title_rx_ifneeded_cl1(@_);
    if (ref($spec) eq 'Regexp') {
      my ($title_rx, $rows) = @$$self{qw/title_rx rows/};
      croak "Can not use regex: No title-row is defined!\n"
        unless defined $title_rx;
      my $title_row = $rows->[$title_rx] // oops;
      my $matched;
      for my $cx (0..$#$title_row) {
        my $title = $title_row->[$cx];
        # Note: We can't use /s here!  The regex compiler has already
        # encapsulated /s or lack thereof in the compiled regex
        if ($title =~ /$spec/) {
          push @results, [$cx, "cx $cx: regex matched title '$title'"];
          $matched++;
        }
      }
      if (! $matched) {
        local $Vis::Maxwidth = 0;
        croak "\n--- Title Row (rx $title_rx) ---\n",
               vis($title_row),"\n-----------------\n",
               "Regex $spec\n",
               "does not match any of the titles (see above) in '$$self->{data_source}'\n"
      }
      next
    }
    croak "Invalid specifier '${spec}'\n"
          ,dvis('$colx');
  }
  oops unless wantarray;
  @results
}
sub _spec2cx {  # return $cx or ($cx, $desc)
  my ($self, $spec) = @_;
  my @list = $self->_specs2cxdesclist($spec);
  if (@list > 1) {
    croak svis("Regexpr $spec matches multiple titles:\n   "),
          join("\n   ",map{ vis $_->[1] } @list), "\n";
  }
  wantarray ? @{$list[0]} : $list[0]->[0]
}

sub _colspec2cx {
  my ($self, $colspec) = @_;
  croak "COLSPEC may not be a regex" if ref($colspec) eq 'Regexp';
  goto &_spec2cx
}

sub spectocxlist { # the user-callable API
  my $self = shift;
  my @list = $self->_specs2cxdesclist(@_);
  checkwantarray(@list); # throws in scalar context if multiple hits
  wantarray ? map{ $_->[0] } @list : $list[0]->[0]
}

# Translate a possibly-relative column specification which 
# indicate 1 off the end.
#
# The specification may be
#   >something  (the column after 'something')
# or 
#   an absolute column indicator (cx or ABC), possibly 1 off the end
# or 
#   refer to an existing column 
#
sub relspec2cx {
  my ($self, $spec) = @_;
  my $colx = $$self->{colx};
  if ($spec =~ /^>(.*)/) {
    my $cx = $self->_colspec2cx($1); # croaks if not an existing column
    return $cx + 1
  }
  if (defined(my $cx = $colx->{$spec})) {
    return $cx 
  }
  if (looks_like_number($spec) && $spec == $$self->{num_cols}) {
    return 0+$spec 
  }
  if ($spec =~ /^[A-Z]+$/ && (my $cx = let2cx($spec)) == $$self->{num_cols}) {
    return $cx
  }
  $self->_colspec2cx($spec); # croaks
  oops; #should not return
}

sub _regexp2cxdesc($) {
  my ($self, $regex) = @_;
  oops unless ref($regex) eq 'Regexp';
  my $matched_title;
  my ($title_rx, $rows) = @$$self{qw/title_rx rows/};
  #croak "No title-row is defined!\n"
  croak "No title-row is defined!\n"
    unless defined $title_rx;
  my $title_row = $rows->[$title_rx] // oops;
  my $cx;
  for my $tcx (0..$#$title_row) {
    my $title = $title_row->[$tcx];
    # Note: We can't apply /s here!  The regex compiler has already
    # encapsulated /s or lack thereof in the compiled regex
    if ($title =~ /$regex/) {
      croak svis "Regexpr ${regex} matches multiple titles:\n",
          "   \$matched_title\n",
          "   \$title\n"
        if defined($matched_title);
      $cx = $tcx;
      $matched_title = $title;
    }
  }
  if (! defined($matched_title)) {
    local $Vis::Maxwidth = 0;
    croak "\n--- Title Row (rx $title_rx) ---\n",
           vis($title_row),"\n-----------------\n",
           "Regex $regex\n",
           "does not match any of the titles (see above) in '$$self->{data_source}'\n",
  }
  my $desc = "cx $cx: regex matched title '$matched_title'";
  return wantarray ? ($cx, $desc) : $cx
}

sub alias {
  my $self = shift;
  croak "alias expects an even number of arguments\n"
    unless scalar(@_ % 2)==0;

  my ($colx, $colx_desc, $useraliases, $rows, $silent)
    = @$$self{qw/colx colx_desc useraliases rows silent/};

  my @cxlist;
  while (@_) {
    my $ident = shift @_;
    my $spec = shift @_;
    croak "identifier is undef!" unless defined $ident;
    croak "identifier is empty" unless $ident ne "";
    croak svisq '$ident is not a valid identifier\n' 
                                       unless $ident eq title2ident($ident);
    croak "Column specifier is undef!" unless defined $spec;

    # _spec2cx will auto-detect title_rx the first time $spec isn't absolute.
    # It returns a 'desc' like "cx N" or "cx N: regex matched title '...'"
    my ($cx, $desc) = $self->_spec2cx($spec); # throws if invalid
    $desc = "alias for ".$desc;
    
    $self->logmethifv(\"$ident => ",\fmt_colspec_cx($spec,$cx));

    my $ocx = eval{ $self->_spec2cx($ident) };
    if (defined($ocx) && $ocx != $cx) {
      my $title_rx = $$self->{title_rx} // oops("impossible cx collision");
      my $title_row = $rows->[$title_rx] // oops;
      if ($title_row->[$ocx] eq $ident) {
        croak("Alias ",vis($ident)," for cx $cx conflicts with a Title of the same name for cx $ocx\n");
      }
    }

    $colx->{$ident} = $cx;
    $colx_desc->{$ident} = $desc;
    $useraliases->{$ident} = 1;
    push @cxlist, $cx;
  }
  $self->rebuild_colx();

  checkwantarray(@cxlist);
  return wantarray ? @cxlist : $cxlist[0];
}

sub unalias(@) {
  my $self = shift;

  my ($colx, $colx_desc, $useraliases)
    = @$$self{qw/colx colx_desc useraliases/};

  foreach (@_) {
    delete $useraliases->{$_} // croak "unalias: '$_' is not a column alias\n";
    $self->logmethifv(\" Removing alias $_ => ", \$colx_desc->{$_});
    delete $colx->{$_} // oops;
    delete $colx_desc->{$_} // oops;
  }
  $self->rebuild_colx();
}

# Set the title row index, or with no arg returns the current value.
#
# If the argument is undef, revert to having not title row.
# If the argument is a number, it specifies the zero-based row rx.
# If the argument is "autodetect", then
#   Optional additional args:
#     required => COLSPEC or [COLSPEC...]  # title(s) which are required
#     max_rx   => NUM,   # maximum rx which may contain the title row
#     first_cx => NUM,   # first column ix which must contain a valid title
#     last_cx  => NUM,   # last column ix which must contain a valid title
#
#   With no <required> titles, the first row with non-empty
#   titles in all positions is used.
#
#   Otherwise the first row containing all required titles (and 
#   non-empty strings in other positions) is used.
#
#   An exception is thrown if a plausible title row can not be found.
#
sub mycallerloc(;$) {
  my ($level) = @_;
  $level //= 0;
  my ($file, $lineno) = (caller($level+1))[1,2];  # +1 for calling this function
  basename($file).":".$lineno
}
sub _warnwithloc {
  my $self = shift;
  my $msg = shift // confess("bug:nomsg");
  my $myloc = mycallerloc(1);
  (my $callingsub = (caller(1))[3]) =~ s/.*:://;
  my $userloc = mycallerloc(1+$$self->{caller_level});
  $msg .= " from $callingsub $myloc   {caller_level}=".vis($$self->{caller_level})
         ." => $userloc\n";
  if ($userloc =~ /Edit.pm/) {
    Carp::cluck dvis $msg
  } else {
    warn dvis $msg
  }
  confess "TOO DEEP" if $$self->{caller_level} > 7;
}
sub title_rx {
  my $self = shift;
  my $title_rx = $$self->{title_rx};
  return $title_rx if @_ == 0;    # 'get' request
  my $rx = shift;
  $self->logmethifv($rx, \" ", \fmt_pairs(@_));
  if (defined $rx) {
    my %opts = @_;
    if ($rx =~ /^auto/i) {  
      { local $$self->{caller_level} = $$self->{caller_level} + 1;
        $self->autodetect_title_rx(%opts); # Pass down max_rx etc.
      }
      $rx = $self->_autodetect_title_rx_ifneeded_cl1(to_array($opts{required}//[])) // oops;
    }
    $self->check_rx($rx);
  } else {
    # Unsetting
    #croak "title_rx was not previously defined\n"
    #  unless defined $title_rx;
    delete $$self->{autodetect_opts};
  }
  $$self->{title_rx} = $rx;
  # N.B. re-scan even if rx has not changed, in case headers were modified
  $self->rebuild_colx();
  $rx;
}

#### USE BY USERS IS DEPRECATED:  Use title_rx("auto", ...) instead
####
# Arrange to auto-detect title_rx the first time titles are needed by
# some operation, for example an "alias" call which specifies a non-absolute 
# COLSPEC, directly or via a regex  (Note: simply de-referencing %col or 
# a potentially-tied column variable is not enough).
#
# Detection looks for the first row which contains non-empty cells in 
# every column (with an optionally-specified range), and which contains
# all needed title(s), if any.
#
# Optional parameters:
#   min_rx, max_rx   => NUM,   # range of rows which may contain the title row.
#   first_cx => NUM,   # first column ix which must contain a valid title
#   last_cx  => NUM,   # last column ix which must contain a valid title
#
# An exception will be thrown later if a plausible title row can not be 
# found when needed.
#
sub autodetect_title_rx {
  # TODO debug this
  my $self = shift;
  my %opts = @_;
  croak "title_rx is already defined ($$self->{title_rx})"
    if defined $$self->{title_rx};
  $$self->{autodetect_opts} = \%opts;
}
sub _autodetect_title_rx_ifneeded_cl1 { 
  # returns title_rx; called from top level.
  # autodetect_title_rx() must have been called previously or else this is a nop
  my ($self, @specs) = @_;
  #{ local $$self->{caller_level} = $$self->{caller_level} + 1; 
  #  $self->_warnwithloc('## DD');
  #}
  local $$self->{caller_level} = $$self->{caller_level} + 2; 
  # increase {caller_level} by 2: One for calling us + one for what we call
#warn dvis '##AA $$self->{title_rx} $$self->{autodetect_opts}\n    @specs';
#local $$self->{verbose} = 1;
#local $$self->{debug} = 1;
  if (! defined($$self->{title_rx}) && defined($$self->{autodetect_opts})) {
    my $verbose = $$self->{verbose};
    my ($opts, $rows, $colx) = @$$self{qw(autodetect_opts rows colx)};
    while(my ($key, $value) = each %$opts) {
      next unless $key =~ /_/;  # ignore 'required' etc.
      croak "Invalid $key value ",vis($value)
        unless defined($value) && $value =~ /^\d+$/
    }
    my $min_rx   = $opts->{min_rx}   // 0;
    my $max_rx   = $opts->{max_rx}   // 3;
    my $first_cx = $opts->{first_cx} // 0;
    my $last_cx  = $opts->{last_cx}  // $$self->{num_cols}-1;
    my ($detected, $nd_reason, $partial_match_rx);
    {
      local $$self->{verbose} = 0; # suppress during trial and error
      local $$self->{silent}  = 1; # 
      RX: for my $rx ($min_rx..min($max_rx,$#$rows)) {
        # Find the first row with all required titles, and non-empty
        # titles in all positions.
        foreach my $spec (@specs) {
          if (eval { 
                local $$self->{caller_level} = $$self->{caller_level} + 1;
                $self->title_rx($rx);
                my @r = $self->_specs2cxdesclist($spec);
              }) {
            $partial_match_rx //= $rx; # for diagnostics
          } else {
            $nd_reason //= "Title $spec not found"; 
            next RX 
          }
        }
        # Unless a specific title was specified, require all title cells
        # to be non-empty 
        unless (@specs > 0) {
          my $row = $rows->[$rx];
          my ($found_nonempty, $empty_cx);
          foreach ($first_cx..$last_cx) {
            if ($row->[$_] eq "") {
              $empty_cx //= $_;
            } else {
              $found_nonempty = 1;
            }
          }
          if (defined $empty_cx) {
            $nd_reason = $found_nonempty ?  fmt_cx($empty_cx)." is empty"
                                         : "all columns are empty";
            next RX
          }
        }
        $detected = $rx;
        last
      }
      $self->title_rx(undef) if $$self->{title_rx}; # will re-do below
    }
    # User's {verbose} & {silent} are restored at this point
    if (defined $detected) {
      carp("Auto-detected title_rx = $detected") if $verbose;
      local $$self->{caller_level} = $$self->{caller_level} + 1;
      local $$self->{verbose} = 0; # suppress normal logging
      $self->title_rx($detected); # might still show collision warnings
    } else {
      $nd_reason //= "No rows checked! (There are ".scalar(@$rows)." in the sheet)";
      $nd_reason .= $min_rx==0 ? " in the first ".($max_rx+1)." rows"
                               : " within rx $min_rx .. rx $max_rx";
      croak("In ",qsh($$self->{data_source})," ...\n",
            "Auto-detect of title_rx failed: $nd_reason",
            (defined($partial_match_rx) ?
               ("\nFYI rx $partial_match_rx contains ",vis($self->rows->[$partial_match_rx]),")\n") : ())
      );
    }
  }
  $$self->{title_rx};
}

#sub forget_title_rx {
#  my $self = shift;
#  if (defined $$self->{title_rx}) {
#    $$self->{title_rx} = undef;
#  } else {
#    croak "forget_title_rx: title_rx is not currently in effect\n";
#  }
#  $self->rebuild_colx(); # remove title-related column variables
#}

sub first_data_rx {
  my $self = shift;
  my $first_data_rx = $$self->{first_data_rx};
  return $first_data_rx if @_ == 0;    # 'get' request
  my $rx = shift;
  $self->logmethifv($rx);
  # Okay if this points to one past the end
  $self->check_rx($rx, 1) if defined $rx;  # onepastendok=1
  $$self->{first_data_rx} = $rx;
  $rx;
}
sub last_data_rx {
  my $self = shift;
  my $last_data_rx = $$self->{last_data_rx};
  return $last_data_rx if @_ == 0;    # 'get' request
  my $rx = shift;
  $self->logmethifv($rx);
  if (defined $rx) {
    $self->check_rx($rx, 1); # onepastendok=1
    confess callingsub(0).": last_data_rx must precede first_data_rx"
      unless $rx >= ($$self->{first_data_rx}//0);
  }
  $$self->{last_data_rx} = $rx;
  $rx;
}

# move_cols ">COLSPEC",source cols...
# move_cols "absolute-position",source cols...
sub move_cols($@) {
  my $self = shift;
  my ($posn, @sources) = @_;

  my ($num_cols, $rows) = @$$self{qw/num_cols rows/};

  my $to_cx = $self->relspec2cx($posn);

  my @source_cxs = map { scalar $self->_spec2cx($_) } @sources;
  my @source_cxs_before = grep { $_ < $to_cx } @source_cxs;
  my $insert_offset = $to_cx - scalar(@source_cxs_before);
  my @rsorted_source_cxs = sort { $b <=> $a } @source_cxs;

  $self->logmethifv(\fmt_colspec_cx($posn,$to_cx), \" <-- ",
                \join(" ",map{"$source_cxs[$_]\[$_\]"} 0..$#source_cxs));

  croak "move destination is too far to the right\n"
    if $to_cx + @sources - @source_cxs_before > $num_cols;

  my @old_cxs = (0..$num_cols-1);

  foreach my $row (@$rows, \@old_cxs) {
    my @moving_cells = @$row[@source_cxs];             # save
    splice @$row, $_, 1 foreach (@rsorted_source_cxs); # delete
    splice @$row, $insert_offset, 0, @moving_cells;    # put back
  };

  $self->adjust_colx(\@old_cxs);
  $self->rebuild_colx();
}
sub move_col { goto &move_cols; }

# new_cols ">COLSPEC",new titles (or ""s or undefs if no title row)
# new_cols "absolute-position",...
# RETURNS: The new column indicies, or in scalar context the first cx
sub new_cols {
  my $self = shift;
  my ($posn, @new_titles) = @_;
  croak "new_cols: At least one new title (or \"\"/undef) must be specified\n"
    unless @new_titles > 0;
  my ($num_cols, $rows, $title_rx) = @$$self{qw/num_cols rows title_rx/};

  @new_titles = map { $_ // "" } @new_titles; # change undef to ""

  if (!defined($title_rx) && grep { $_ ne "" } @new_titles) {
    $title_rx = $self->_autodetect_title_rx_ifneeded_cl1();
    croak "new_cols: Can not specify titles if no title_rx is defined\n"
      if !defined($title_rx);
  }
  my $num_new_cols = @new_titles;
  my $to_cx = $self->relspec2cx($posn);

  $self->logmethifv(\fmt_colspec_cx($posn,$to_cx), \" <-- ", \avis(@new_titles));

  foreach my $row (@$rows) {
    if (defined $title_rx && $row == $rows->[$title_rx]) {
      splice @$row, $to_cx, 0, @new_titles;
    } else {
      splice @$row, $to_cx, 0, (("") x $num_new_cols);
    }
  }
  $$self->{num_cols} += $num_new_cols;

  $self->adjust_colx(
    [ 0..$to_cx-1, ((undef) x $num_new_cols), $to_cx..$num_cols-1 ]
  );
  #$self->rebuild_colx() if grep {$_ ne ""} @new_titles;
  $self->rebuild_colx();

  # Assume user never intends to receive only one of multiple cx values
  croak "new_cols called in scalar context but multiple column indicies would be returned\n"
    if $num_new_cols> 1 && defined(wantarray) && !wantarray;

  return wantarray ? ($to_cx..$to_cx+$num_new_cols-1) : $to_cx;
}
sub new_col { goto &new_cols }

# sort_rows {compare function taking two row indicies} 
# sort_rows {compare function taking two row indicies} $first_rx, $last_rx
sub sort_rows {
  my $self = shift;
  croak "bad args" unless @_ == 1;
  my ($cmpfunc, $first_rx, $last_rx) = @_;

  my ($rows, $linenums, $title_rx, $first_data_rx, $last_data_rx) 
       = @$$self{qw/rows linenums title_rx first_data_rx last_data_rx/};

  $first_rx //= $first_data_rx
                 // (defined($title_rx) ? $title_rx+1 : 0);
  $last_rx  //= $last_data_rx // $#$rows;

  oops unless defined($first_rx);
  oops unless defined($last_rx);
  #my @indicies = sort { $cmpfunc->($a,$b) } ($first_rx..$last_rx);
  my @indicies = do{
    my $pkg = $self->callers_pkg;
    no strict 'refs';
    local (${ $pkg."::a" }, ${ $pkg."::b" });
    my $ref2a = \${ $pkg."::a" };
    my $ref2b = \${ $pkg."::b" };
    # The row indicies are passed as arguments;
    # The actual rows are passed in globals $a and $b
    sort { $$ref2a=$$rows[$a]; $$ref2b=$$rows[$b]; $cmpfunc->($a,$b) } 
         ($first_rx..$last_rx);
  };

  #TODO: Consider changing API to pass row objects as $a and $b
  #      (still passing row indicies as args)

  @$rows[$first_rx..$#$rows] = @$rows[@indicies];
  @$linenums[$first_rx..$#$rows] = @$linenums[@indicies];

  return unless defined wantarray;
  croak "sort_rows returns an array, not scalar" unless wantarray;
  # Return array mapping new row indicies to old row indicies
  (0..$first_rx-1, @indicies, $last_rx+1..$#$rows)
}
sub sort_csv { oops "sort_csv is an obsolete api" }

sub delete_cols {
  my $self = shift;
  my (@cols) = @_;
  my ($num_cols, $rows) = @$$self{qw/num_cols rows/};

  my @cxlist = $self->colspecs_to_cxlist_chkunique(\@cols);

  my @reverse_cxs = sort { $b <=> $a } @cxlist;

  $self->logmethifv(reverse @reverse_cxs);
  my @old_cxs = (0..$num_cols-1);
  for my $row (@$rows, \@old_cxs) {
    foreach my $cx (@reverse_cxs) {
      oops if $cx > $#$row;
      splice @$row, $cx, 1, ();
    }
  }
  $$self->{num_cols} -= @reverse_cxs;
  $self->adjust_colx(\@old_cxs);
  $self->rebuild_colx();
}
sub delete_col { goto &delete_cols; }

# Set option(s), returning the previous value (of the last one specified)
sub options {
  my $self = shift;
  my $prev;
  if (@_==1 && ref($_[0]) eq 'HASH') { @_ = %{$_[0]} } 
  while (@_ > 0) {
    my $key = shift;
    my $val = shift(@_) if @_ > 0; # else undef
    $prev = $$self->{$key};  
    if ($key eq "silent") { 
      if (defined $val) {
        $$self->{$key} = $val;
      }
    }
    elsif ($key eq "verbose") { 
      if (defined $val) {
        $$self->{verbose} = $val;
        $$self->{silent} = undef if $val;
      }
    }
    elsif ($key eq "debug") { 
      if (defined $val) {
        $$self->{debug}   = $val;
        $$self->{verbose } = 1 if $val;
        $$self->{silent} = undef if $val;
      }
    }
    else { croak "options: Unknown option key '$key' (possible keys: silent verbose debug)\n"; }
  }
  $prev;
}

sub colspecs_to_cxlist_chkunique {
  my ($self, $colspecs) = @_; oops unless @_==2;
  my @cxlist;
  my %seen;
  foreach (@$colspecs) {
    my $cx = $self->_spec2cx($_);  # auto-detects title_rx if needed
    if ($seen{$cx}) {
      croak "cx $cx is specified by multiple COLSPECs: ", vis($_)," and ",vis($seen{$cx}),"\n";
    }
    $seen{ $cx } = $_;
    push @cxlist, $cx;
  }
  @cxlist
}

sub only_cols {
  my ($self, @cols) = @_;
  my $rows = $self->rows;

  # Replace each row with just the surviving columns, in the order specified
  my @cxlist = $self->colspecs_to_cxlist_chkunique(\@cols);
  #use Vis; warn dvis '##A @cxlist\n';
  for my $row (@$rows) {
    @$row = map{ $row->[$_] } @cxlist;
  }
  $$self->{num_cols} = scalar(@cxlist);
  #use Vis; warn '##B :',vis($self->colx),"\n";
  $self->adjust_colx(\@cxlist);
  #use Vis; warn '##C :',vis($self->colx),"\n";
  $self->rebuild_colx();
  #use Vis; warn '##D :',vis($self->colx),"\n";
}

# obj->join_cols separator_or_coderef, colspecs...
# If coderef:
#   $_ is bound to the first-named column, and is the destination
#   @_ is bound to all named columns, in the order named.
sub join_cols {
  my $self = shift;
  my ($separator, @sources) = @_;
  my $hash = $$self;

  my ($num_cols, $rows) = @$hash{qw/num_cols rows/};

  my @source_cxs = map { scalar $self->_spec2cx($_) } @sources;
  $self->logmethifv(\"'$separator' ",
                \join(" ",map{"$source_cxs[$_]\[$_\]"} 0..$#source_cxs));

  my $saved_v = $hash->{verbose}; $hash->{verbose} = 0;

  # Merge the content into the first column.  N.B. EXCLUDES title row.
  my $code = ref($separator) eq 'CODE' 
               ? $separator
               : sub{ $_ = join $separator, @_ } ;

  # Note first/last_data_rx are ignored
  { my $first_rx = ($hash->{title_rx} // -1)+1;
    local $$self->{caller_level} = $$self->{caller_level} + 1;
    _apply_to_rows($self, $code, \@source_cxs, undef, $first_rx, undef);
  }

  # Delete the other columns
  $self->delete_cols(@source_cxs[1..$#source_cxs]);

  $$self->{verbose} = $saved_v;
}

sub rename_cols(@) {
  my $self = shift;
  croak "rename_cols expects an even number of arguments\n"
    unless scalar(@_ % 2)==0;
  my $pkg = $self->callers_pkg;

  my ($num_cols, $rows, $title_rx) = @$$self{qw/num_cols rows title_rx/};

  if (!defined $title_rx) {
    $title_rx = $self->_autodetect_title_rx_ifneeded_cl1();
    croak "rename_cols: No title_rx is defined!\n" if !defined($title_rx);
  }
  my $title_row = $rows->[$title_rx];

  while (@_) {
    my $old_title = shift @_;
    my $new_title = shift @_;
    my $cx = $self->_spec2cx($old_title);
    $self->logmethifv($old_title, \" -> ", $new_title, \" [cx $cx]");
    croak "rename_cols: Column $old_title is too large\n"
      if $cx > $#$title_row; # it must have been an absolute form
    $title_row->[$cx] = $new_title;

    # N.B. aliases remain pointing to the same columns regardless of names
  }
  $self->rebuild_colx();
}

# apply {code}, colspec*
#   @_ are bound to the columns in the order specified (if any)
#   $_ is bound to the first such column
#   Only visit rows bounded by first_data_rx and/or last_data_rx,
#   starting with title_rx+1 if a title row is defined.
sub apply {
  my $self = shift;
  my ($code, @cols) = @_;
  my $hash = $$self;
  my @cxs = map { scalar $self->_spec2cx($_) } @cols;

  $self->_autodetect_title_rx_ifneeded_cl1() if !defined $hash->{title_rx};

  my $first_rx = max(($hash->{title_rx} // -1)+1, $hash->{first_data_rx}//0);

  @_ = ($self, $code, \@cxs, undef, $first_rx, $hash->{last_data_rx});
  goto &_apply_to_rows
}

# apply_all {code}, colspec*
#  Like apply, but ALL rows are visited, inluding the title row if any
sub apply_all {
  my $self = shift;
  my ($code, @cols) = @_;
  my $hash = $$self;
  my @cxs = map { scalar $self->_spec2cx($_) } @cols;
  $self->logmethifv(\"rx 0..",$#{$hash->{rows}},
                    @cxs > 0 ? \(" cxs=".avis(@cxs)) : ());
  @_ = ($self, $code, \@cxs);
  goto &_apply_to_rows
}

sub ArrifyCheckNotEmpty($) {
  local $_ = shift;
  return $_ if ref($_) eq 'ARRAY'; # already an array reference
  croak "Invalid argument ",vis($_)," (expecting [array ref] or single value)\n"
    unless defined($_) && $_ ne "";
  return [ $_ ];
}

# apply_torx {code} rx,        colspec*
# apply_torx {code} [rx list], colspec*
# Only the specified row(s) are visited
# first/last_data_rx are ignored.
sub apply_torx {
  my $self = shift;
  my ($code, $rxlist_arg, @cols) = @_;
  croak "Missing rx (or [list of rx]) argument\n" unless defined $rxlist_arg;
  my $rxlist = ArrifyCheckNotEmpty($rxlist_arg);
  my @cxs = map { scalar $self->_spec2cx($_) } @cols;
  $self->logmethifv(\"rxs=",\vis($rxlist_arg),
                    @cxs > 0 ? \(" cxs=".avis(@cxs)) : ());
  @_ = ($self, $code, \@cxs, $rxlist);
  goto &_apply_to_rows
}

# apply_exceptrx {code} [rx list], colspec*
# All rows EXCEPT the specified rows are visited
sub apply_exceptrx {
  my $self = shift;
  my ($code, $exrxlist_arg, @cols) = @_;
  croak "Missing rx (or [list of rx]) argument\n" unless defined $exrxlist_arg;
  my $exrxlist = ArrifyCheckNotEmpty($exrxlist_arg);
  my @cxs = map { scalar $self->_spec2cx($_) } @cols;
  my $hash = $$self;
  my $max_rx = $#{ $hash->{rows} };
  foreach (@$exrxlist) {
    croak "rx $_ is out of range\n" if $_ < 0 || $_ > $max_rx;
  }
  my %exrxlist = map{ $_ => 1 } @$exrxlist;
  my $rxlist = [ grep{ ! exists $exrxlist{$_} } 0..$max_rx ];
  $self->logmethifv(\vis($exrxlist_arg), 
                    @cxs > 0 ? \(" cxs=".avis(@cxs)) : ());
  @_ = ($self, $code, \@cxs, $rxlist);
  goto &_apply_to_rows
}

# split_col {code} oldcol, newcol_start_position, new titles...
#  {code} is called for each row with $_ bound to <oldcol>
#         and @_ bound to the new column(s).
# The old column is left as-is (not deleted).
sub split_col {
  my $self = shift;
  my ($code, $oldcol_posn, $newcols_posn, @new_titles) = @_;

  my $num_new_cols = @new_titles;
  my $old_cx = $self->_spec2cx($oldcol_posn);
  my $newcols_first_cx = $self->relspec2cx($newcols_posn);

  $self->logmethifv(\"... $oldcol_posn\[$old_cx] -> [$newcols_first_cx]",
                    avis(@new_titles));
  my $saved_v = $$self->{verbose}; $$self->{verbose} = 0;

  $self->new_cols($newcols_first_cx, @new_titles);

  $old_cx += $num_new_cols if $old_cx >= $newcols_first_cx;

  $self->apply($code,
               $old_cx, $newcols_first_cx..$newcols_first_cx+$num_new_cols-1);

  $$self->{verbose} = $saved_v;
}

sub reverse_cols() {
  my $self = shift;
  my ($rows, $num_cols) = @$$self{qw/rows num_cols/};
  $self->logmethifv();
  for my $row (@$rows) {
    @$row = reverse @$row;
  }
  $self->adjust_colx([reverse 0..$num_cols-1]);
  $self->rebuild_colx();
}

sub invert() {
  my $self = shift;
  $self->logmethifv();

  my ($rows, $num_cols) = @$$self{qw/rows num_cols/};

  $$self->{useraliases} = {};
  $$self->{title_rx} = undef;
  $$self->{first_data_rx} = undef;
  $$self->{last_data_rx} = undef;

  my @new_rows;
  for (my $cx=0; $cx < $num_cols; ++$cx) {
    my $new_row = [];
    for my $row (@$rows) {
      push @$new_row, $row->[$cx] // "";
    }
    push @new_rows, $new_row;
  }
  $$self->{num_cols} = @$rows;
  @$rows = @new_rows;
  @{ $$self->{linenums} } = ("??") x @$rows;
  $$self->{data_source} .= " inverted";
  undef $$self->{num_cols};

  $self->_newdata;
}

# delete_rows rx ...
# delete_rows 'LAST' ...
# delete_rows '$' ...
sub delete_rows {
  my $self = shift;
  my (@rowspecs) = @_;

  my ($rows, $linenums, $title_rx, $first_data_rx, $last_data_rx, $current_rx, $verbose)
    = @$$self{qw/rows linenums title_rx first_data_rx last_data_rx current_rx verbose/};

  $title_rx //= $self->_autodetect_title_rx_ifneeded_cl1();

  foreach (@rowspecs) {
    $_ = $#$rows if /^(?:LAST|\$)$/;
    croak "Invalid row index '$_'\n" unless /^\d+$/ && $_ <= $#$rows;
  }
  my @rev_sorted_rxs = sort {$b <=> $a} @rowspecs;
  $self->logmethifv(reverse @rev_sorted_rxs);

  # Adjust if needed...
  if (defined $title_rx) {
    foreach (@rev_sorted_rxs) {
      if ($_ < $title_rx) { --$title_rx }
      elsif ($_ == $title_rx) {
        $self->log("Invalidating titles because rx $title_rx is being deleted\n")
          if $$self->{verbose};
        $title_rx = undef;
        last;
      }
    }
    $$self->{title_rx} = $title_rx;
  }
  if (defined $first_data_rx) {
    foreach (@rev_sorted_rxs) {
      if ($_ <= $first_data_rx) { --$first_data_rx }
    }
    $$self->{first_data_rx} = $first_data_rx;
  }
  if (defined $last_data_rx) {
    foreach (@rev_sorted_rxs) {
      if ($_ <= $last_data_rx) { --$last_data_rx }
    }
    $$self->{last_data_rx} = $last_data_rx;
  }

  # Back up $current_rx to account for deleted rows.
  # $current_rx is left set to one less than the index of the "next" row if
  # we are in an apply().  That is, current_rx will be left still pointing to
  # the same row as before, or if that row has been deleted then the row
  # before that (or -1 if row zero was deleted).
  if (defined $current_rx) {
    foreach (@rev_sorted_rxs) {
      --$current_rx if ($_ <= $current_rx);
    }
    $$self->{current_rx} = $current_rx;
  }

  #warn "### BEFORE delete_rows rx (@rev_sorted_rxs):\n",
  #     map( { "   [$_]=(".join(",",@{$rows->[$_]}).")\n" } 0..$#$rows);

  for my $rx (@rev_sorted_rxs) {
    splice @$rows, $rx, 1, ();
    splice @$linenums, $rx, 1, ();
  }

  #warn "### AFTER delete_rows:\n",
  #     map( { "   [$_]=(".join(",",@{$rows->[$_]}).")\n" } 0..$#$rows);
}
sub delete_row { goto &delete_rows; }

# $firstrx = new_rows [rx [,count]]
# $firstrx = new_rows ['$'[,count]]
sub new_rows {
  my $self = shift;
  my ($rx, $count) = @_;
  $rx //= '$';
  $count //= 1;

  my ($rows, $linenums, $num_cols, $title_rx, $first_data_rx, $last_data_rx)
    = @$$self{qw/rows linenums num_cols title_rx first_data_rx last_data_rx/};

  $rx = @$rows if $rx eq '$';

  $self->logmethifv(\"at rx $rx (count $count)");

  if (defined($title_rx) && $rx <= $title_rx) {
    $title_rx += $count;
    $$self->{title_rx} = $title_rx;
  }
  if (defined($first_data_rx) && $rx <= $first_data_rx) {
    $first_data_rx += $count;
    $$self->{first_data_rx} = $first_data_rx;
  }
  if (defined($last_data_rx) && $rx <= $last_data_rx) {
    $last_data_rx += $count;
    $$self->{last_data_rx} = $last_data_rx;
  }

  for (1..$count) {
    splice @$rows, $rx, 0, [("") x $num_cols];
    splice @$linenums, $rx, 0, -1;
  }

  return $rx;
}
sub new_row { goto &new_rows; }

# read_spreadsheet $inpath [Text::CSV::Spreadsheet options...]
# read_spreadsheet $inpath [,iolayers =>...  or encoding =>...] 
# read_spreadsheet $inpath [,{iolayers =>...  or encoding =>... }] #OLD API
sub read_spreadsheet {
  my $self = shift;
  my $inpath;
  my %opts = $self->process_args(\@_, "inpath" => \$inpath);

  my %csvopts = @sane_CSV_read_options;
  # Separate out Text::CSV options from %opts
  foreach my $key (Text::CSV::known_attributes()) {
    #$csvopts{$key} = delete $opts{$key} if exists $opts{$key};
    $csvopts{$key} = $opts{$key} if defined $opts{$key};
    delete $opts{$key};
  }
  $csvopts{escape_char} = $csvopts{quote_char}; # " →  ""

  check_opthash(\%opts,[],[
          qw/iolayers encoding verbose silent debug/,
          # N.B. This used to include 'quiet' but it did not do anything
          qw/tempdir use_gnumeric/,
          qw/sheet/, # for Text::CSV::OpenAsCsv
          ]);
  

  # convert {encoding} to {iolayers}
  if (my $enc = delete $opts{encoding}) {
    #warn "Found OBSOLETE read_spreadsheet 'encoding' opt (use iolayers instead)\n";
    $opts{iolayers} = ($opts{iolayers}//"") . ":encoding($enc)";
  }
  # Same as last-used, if any
  # N.B. If user says nothing, OpenAsCsv() defaults to UTF-8
  $opts{iolayers} //= $$self->{iolayers} // "";

  my ($rows, $linenums, $meta_info, $verbose, $debug)
    = @$$self{qw/rows linenums meta_info verbose debug/};

  ##$self->_check_currsheet;

  my $hash;
  { local $$self->{verbose} = 0;
    $hash = OpenAsCsv(
                   path => $inpath,
                   debug => $$self->{debug},
                   verbose => ($$self->{verbose} || $$self->{debug}),
                   %opts, # all our opts are valid here
             );
  }
  $self->logmethifv($inpath, $hash);

  # Save possibly-defaulted iolayers for use in subsequent write_csv
  $$self->{iolayers} //= $hash->{iolayers};

  my $fh = $hash->{fh};

  $csvopts{keep_meta_info} = 1;
  my $csv = Text::CSV->new (\%csvopts)
              or croak "read_spreadsheet: ".Text::CSV->error_diag ()
                      .dvis('\n## %csvopts\n');

  @$rows = ();
  @$linenums = ();
  my $lnum = 1;
  while (my $F = $csv->getline( $fh )) {
    push(@$linenums, $lnum);
    my @minfo = $csv->meta_info();
    # Force quoting of fields which look like negative numbers with an ascii 
    # minus (\x{2D}) rather than Unicode math minus (\N{U+2212}).  
    # This prevents conversion to the Unicode math minus when LibreOffice 
    # reads the CSV.  The assumption is that if the input, when converted 
    # TO a csv, has an ascii minus then the original spreadsheet cell format 
    # was "text" not numeric.
    for my $cx (0..$#$F) {
      #...TODO   $minfo[$cx] |= 0x0001 if $F->[$cx] =~ /^-[\d.]+$/a;
    }
    push(@$meta_info, \@minfo);
    $lnum = $.+1;
    push(@$rows, $F);
  }
  close $fh || croak "Error reading $hash->{csvpath}: $!\n";

  $$self->{data_source} = $inpath;
  $$self->{sheetname} = $hash->{sheet};
  
  undef $$self->{num_cols};
  $self->_newdata;
}

# write_csv "/path/to/output.csv"
# write_csv "/path/to/output.csv", opt1 => val1, ...
# write_csv "/path/to/output.csv", {option hash} # deprecated
# write_csv "/path/to/output.csv", {option hash} {csvoption hash} # NO LONGER
sub write_csv {
  my $self = shift;
  my $outpath;
  my %opts = $self->process_args(\@_, "outpath" => \$outpath);

  # Cells will be quoted if the input was quoted, i.e. if indicated by meta_info.
  my %csvopts = ( @sane_CSV_write_options,
                  quote_space => 0,  # dont quote embedded spaces
                );
  # Separate out Text::CSV options from args
  foreach my $key (Text::CSV::known_attributes()) {
    $csvopts{$key} = delete $opts{$key} if exists $opts{$key};
  }
  
  $opts{iolayers} //= $$self->{iolayers} // "";
  # New API: opts->{iolayers} may have all 'binmode' arguments.
  # If it does not include encoding(...) then insert default
  if ($opts{iolayers} !~ /encoding\(|:utf8/) {
    $opts{iolayers} .= ":encoding(".
            ($self->input_encoding() || DEFAULT_WRITE_ENCODING)
                                     .")";
  }
  if ($opts{iolayers} !~ /:(?:crlf|raw)\b/) {
    # Use platform default
    #$opts{iolayers} .= ":crlf";
  }

  my ($rows, $meta_info, $num_cols, $verbose, $debug) 
    = @$$self{qw/rows meta_info num_cols verbose debug/};

  local $Text::CSV::Spreadsheet::verbose ||= $verbose;
  local $Text::CSV::Spreadsheet::debug   ||= $debug;

  my $fh;
  if (ref $outpath) { # an already-open file handle?
    $self->logmethifv(\("<file handle specified> $opts{iolayers} "
                                .scalar(@$rows)." rows, $num_cols columns)"));
    $fh = $outpath;
  } else {
    $self->logmethifv(\($outpath." $opts{iolayers} ("
                                .scalar(@$rows)." rows, $num_cols columns)"));
    croak "Output path suffix must be *.csv, not\n  ",qsh($outpath),"\n"
      if $outpath =~ /\.([a-z]*)$/ && $1 !~ /^csv$/i;
    open $fh,">$outpath" or croak "$outpath: $!\n";
  }

  binmode $fh, $opts{iolayers} or die "binmode:$!";

  # Arrgh.  Although Text::CSV is huge and complex and implements a complicated
  # meta_info mechanism to capture quoting details on input, there is no way to
  # use the captured info to specify quoting of output fields!
  # So we implement writing CSVs by hand here.
  #my $csv = Text::CSV->new (\%csvopts)
  #            or die "write_csv: ".Text::CSV->error_diag ();
  #foreach my $row (@$rows) {
  #  oops "UNDEF row" unless defined $row;  # did user modify @rows?
  #  $csv->print ($fh, $row);
  #};
  
  # Much of the option handling code was copied from Text::CSV_PP.pm
  # which depends on default values of options we don't specify explicitly.
  # So create a Text::CSV object just to get the effective option values...
  { my $o = Text::CSV->new( \%csvopts );
    foreach my $key (Text::CSV::known_attributes()) {
      $csvopts{$key} = $o->{$key};
    }
  }
  
  my $re_esc = ($csvopts{escape_char} ne '' and $csvopts{escape_char} ne "\0") 
                 ? ($csvopts{quote_char} ne '') ? qr/(\Q$csvopts{quote_char}\E|\Q$csvopts{escape_char}\E)/ : qr/(\Q$csvopts{escape_char}\E)/
                 : qr/(*FAIL)/;
  for my $rx (0..$#$rows) {
    my $row = $rows->[$rx];
    my $minfo = $meta_info->[$rx];
    my @results;
    for my $cx (0..$num_cols-1) {
      my $value = $row->[$cx];
      confess "ERROR: rx $rx, cx $cx : undef cell value" unless defined($value);
      my $mi = $minfo->[$cx]; # undef if input was missing columns in this row
      my $must_be_quoted = $csvopts{always_quote} ||
                             (($mi//0) & 0x0001); # was quoted on input
      unless ($must_be_quoted) {
        if ($value eq '') {
          $must_be_quoted = 42 if $csvopts{quote_empty};
        } else {
          if ($csvopts{quote_char} ne '') {
            use bytes;
            $must_be_quoted=43 if
                    ($value =~ /\Q$csvopts{quote_char}\E/) ||
                    ($csvopts{sep_char} ne '' and $csvopts{sep_char} ne "\0" and $value =~ /\Q$csvopts{sep_char}\E/) ||
                    ($csvopts{escape_char} ne '' and $csvopts{escape_char} ne "\0" and $value =~ /\Q$csvopts{escape_char}\E/) ||
                    ($csvopts{quote_binary} && $value =~ /[\x00-\x1f\x7f-\xa0]/) ||
                    ($csvopts{quote_space} && $value =~ /[\x09\x20]/);
          }
        }
      }
      $value =~ s/($re_esc)/$csvopts{escape_char}$1/g;
      if ($csvopts{escape_null}) {
        $value =~ s/\0/$csvopts{escape_char}0/g;
      }
      if ($must_be_quoted) {
        #use Vis; warn dvis '## $csvopts{always_quote} $must_be_quoted $value\n';
        $value = $csvopts{quote_char} . $value . $csvopts{quote_char};
      }
      $fh->print($csvopts{sep_char}) unless $cx==0;
      $fh->print($value);
    }
    $fh->print($csvopts{eol});
  }

  if (! ref $outpath) {
    close $fh || croak "Error writing $outpath: $!\n";
  }
}

# Write spreadsheet with specified column formats
# {col_formats} is required
# Unless {sheet} is specified, the sheet name is the outpath basename
#   sans any suffix
sub write_spreadsheet {
  my $self = shift;
  my $outpath;
  my %opts = $self->process_args(\@_, "outpath" => \$outpath);
  my $colx = $$self->{colx};

  $self->logmethifv($outpath);

  # {col_formats} may be [list of formats in column order]
  #   or { COLSPEC => fmt, ..., __DEFAULT__ => fmt } 
  # Transform the latter to the former...
  my $cf = $opts{col_formats} // croak "{col_formats} is required";
  if (ref($cf) eq "HASH") {
    my ($default, @ary);
    while (my ($key, $fmt) = each %$cf) {
      ($default = $fmt),next if $key eq "__DEFAULT__";
      my $cx = $colx->{$key} // croak("Invalid COLSPEC '$key' in col_formats");
      $ary[$cx] = $fmt;
    }
    foreach (@ary) { $_ = $default if ! defined; }
    $cf = \@ary;
  }
  local $opts{col_formats} = $cf;

  my ($csvfh, $csvpath) = tempfile(SUFFIX => ".csv");
  { local $$self->{verbose} = 0;
    $self->write_csv($csvfh, silent => 1, iolayers => ':encoding(UTF-8)',
                             @sane_CSV_write_options);
  }
  close $csvfh or die "Error writing $csvpath : $!";

  # Default sheet name to output file basename sans suffix
  $opts{sheet} //= fileparse($outpath, qr/\.\w+/);

  convert_spreadsheet($csvpath, 
                      %opts,
                      iolayers => ':encoding(UTF-8)',
                      cvt_from => "csv",
                      outpath => $outpath,
                     );
}

# Analogous to write_csv(), but writes fixed-width records to a file.
# All fields are padded to specified widths, throwing an error if a field
# value is wider than the maximum width.  Then field values are simply 
# written to the file with no field separators, and a specified 
# record separator (which may be the null string "").
#
# If a file handle is passed, it should be in an appropriate text mode
# if newlines are included in the data or record separator.
#
# Required options:
#   record_sep => STRING,
#   widths => [NUMBER,...],  # for each column
# Optional options:
#   padchar => CHAR    # use instead of space
#
sub write_fixedwidth {
  my $self = shift;
  croak("write_fixedwidth: Too many arguments\n") if @_ > 2;
  my ($outpath, $opts) = @_;
  $opts = {encoding => $opts} unless ref $opts; # support old API

  croak "{record_sep} is required (specify \"\" for no separator)." 
    unless defined $opts->{record_sep};

  croak "{widths} is required"
    unless ref($opts->{widths}) eq "ARRAY";

#  # New API: opts->{iolayers} may have all 'binmode' arguments.
#  # If it does not include encoding(...) then opts->{encoding}
#  # is used if present, else defaults.
#  #
#  $opts->{iolayers} //= $$self->{iolayers} // "";
#  if ($opts->{iolayers} !~ /encoding\(|:utf8/) {
#    $opts->{iolayers} .= ":encoding(".
#            ($self->input_encoding() || DEFAULT_WRITE_ENCODING)
#                                     .")";
#  }
#  if ($opts->{iolayers} !~ /:crlf\b/) {
#    $opts->{iolayers} .= ":crlf";
#  }
#
#  my ($rows, $num_cols, $verbose, $debug) = @$$self{qw/rows num_cols verbose debug/};
#
#  my $fh;
#  if (ref $outpath) { # an already-open file handle?
#    $self->logmethifv("<file handle specified> $opts->{iolayers}");
#    $fh = $outpath;
#  } else {
#    $self->logmethifv("$outpath $opts->{iolayers} : ",
#                      scalar(@$rows), " rows, $num_cols columns, ",
#                     );
#
#    open $fh,">$outpath" or croak "$outpath: $!\n";
#  }
#
#  binmode $fh, $opts->{iolayers} or die "binmode:$!";
#
#  my $csv = Text::CSV->new ($TextCSVoptions)
#              or die "write_fixedwidth: ".Text::CSV->error_diag ();
#  foreach my $row (@$rows) {
#    oops "UNDEF row" unless defined $row;  # did user modify @rows?
#    $csv->print ($fh, $row);
#  };
#
#  if (! ref $outpath) {
#    close $fh || croak "Error writing $outpath: $!\n";
#  }
}

1;
