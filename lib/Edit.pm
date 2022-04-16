# License: http://creativecommons.org/publicdomain/zero/1.0/
# (CC0 or Public Domain).  To the extent possible under law, the author, 
# Jim Avera (email jim.avera at gmail dot com) has waived all copyright and 
# related or neighboring rights to this document.  Attribution is requested
# but not required.

# $Id: Edit.pm,v 1.115 2022/04/01 02:34:15 jima Exp jima $

# Pod documentation is below (use perldoc to view)

use strict; use warnings FATAL => 'all';
use feature qw(state switch);

package Text::CSV::Edit;
use Text::CSV::Edit::OO qw(%pkg2currsheet);
require Exporter;

#use vars '@CARP_NOT'; @CARP_NOT = ('Text::CSV::Edit::OO');

use vars qw/@EXPORT @EXPORT_OK @ISA %EXPORT_TAGS/;

# Allow users to access the 'new' method directly, hiding the OO package.
# This also makes Carp::croak not show trampoline functions.
@ISA = (__PACKAGE__."::OO");

@EXPORT = qw(
alias apply apply_all apply_exceptrx apply_torx attributes autodetect_title_rx
spectocxlist data_source delete_col delete_cols delete_row delete_rows
first_data_rx invert join_cols join_cols_sep last_data_rx move_col
move_cols new_col new_cols new_row new_rows new_sheet only_cols options
package_active_sheet read_spreadsheet rename_cols reverse_cols sheet sheetname
sort_rows split_col tie_column_vars title2ident title_row title_rx
unalias untie_column_vars write_csv write_spreadsheet
);

my @stdvars = qw(
      @rows @linenums @meta_info $num_cols
      %colx %colx_desc $title_rx $title_row
      $first_data_rx $last_data_rx
      $row $rx $linenum
      %rowhash %row
);

# N.B. logmsg() is not exported by default
@EXPORT_OK = (@stdvars, qw(tie_sheet_vars logmsg));
%EXPORT_TAGS = (
      STDVARS => [@stdvars],
);

# Trivial use of the procedural interface can be just
#
#    use Text::CSV::Edit qw(:DEFAULT :STDVARS);
#    read_spreadsheet "file.csv";
#    apply {
#      if ($row->{Title} eq "John Brown") {  ... }
#    }
#
# The globals $row, $rx and friends have to be "imported" by the 'use',
# but actually they must be separate tied variables in the user's package,
# NOT aliased to variables in Text::CSV::Edit (because then all packages 
# would share a single 'current sheet').

# Unfortunately Exporter is not factored in a way which lets us use it
# to process :TAG and other options and still get control to do the actual
# "exporting" ourself (or even know exactly which symbols were exported).  
# So we have to do everything here.

use Carp;
use Guard qw(scope_guard guard);
use Vis;
#sub u(@) { unshift(@_,$_) unless @_; (defined($_[0]) ? $_[0] : "undef") }

sub import {
    # TODO:
    # Instead of using code copied from Exporter internals, it might be 
    # better to examine the args and *predict* which variables
    # Exporter::import would import, filter out the special tied variables,
    # and then just call Exporter::import to handle the regular ones.
    # That way we won't depend so much on Exporter internals.
    my $pkg = shift;  # that's us
    my $callpkg = caller($Exporter::ExportLevel);
    my @imports = @_;
    my ($exports, $export_cache) = do{
      no strict 'refs';
      (\@{"${pkg}::EXPORT"}, $Exporter::Cache{$pkg} ||= {})
    };
    if (@_ == 0) {
            @imports = @$exports;
    } else {
            @imports = @_;
            # the following was taken from Exporter::Heavy::heavy_export()
            my $tagsref = do{ no strict 'refs'; \%{"${pkg}::EXPORT_TAGS"} };
            my $tagdata;
            my %imports;
            my($remove, $spec, @names, @allexports);
            # negated first item implies starting with default set:
            unshift @imports, ':DEFAULT' if $imports[0] =~ m/^!/;
            foreach $spec (@imports){
                $remove = $spec =~ s/^!//;

                if ($spec =~ s/^://){
                    if ($spec eq 'DEFAULT'){
                        @names = @$exports;
                    }
                    elsif ($tagdata = $tagsref->{$spec}) {
                        @names = @$tagdata;
                    }
                    else {
                        carp qq["$spec" is not defined in %${pkg}::EXPORT_TAGS];
                        next;
                    }
                }
                elsif ($spec =~ m:^/(.*)/$:){
                    my $patn = $1;
                    if (@allexports == 0) {
                      @allexports = @$exports;
                      if (my $ok = \@{"${pkg}::EXPORT_OK"}) {
                        push @allexports, @$ok;
                      }
                      s/^&// foreach @allexports;
                    }
                    @names = grep(/$patn/, @allexports); # not anchored by default
                }
                else {
                    @names = ($spec); # is a normal symbol name
                }

                warn "Import ".($remove ? "del":"add").": @names "
                    if $Exporter::Verbose;

                if ($remove) {
                   foreach my $sym (@names) { delete $imports{$sym} } 
                }
                else {
                    @imports{@names} = (1) x @names;
                }
            }
            @imports = keys %imports;
    }
    Exporter::Heavy::_rebuild_cache($pkg, $exports, $export_cache);

    my @carp;
    foreach my $sym (@imports) {
      push @carp, qq["$sym" is not exported by the $pkg package\n]
        if !$export_cache->{$sym};
    }
    Carp::croak("@{carp}Can't continue after import errors") if @carp;

    # Separate out the special tied "sheet variables"
    my %is_sheetvar = map{ $_ => 1 } @stdvars;
    my @sheetvar_imports;
    my @regular_imports = ();
    foreach (@imports) {
      if ($is_sheetvar{$_}) {
        push @sheetvar_imports, $_;
      } else {
        push @regular_imports, $_;
      }
    }
    @imports = @regular_imports;

    warn "Importing into $callpkg from $pkg: ",
		join(", ",sort @imports) if $Exporter::Verbose;

    foreach my $sym (@imports) {
        $sym =~ s/^(\W)//;
	my $type = $1; # undef if no type char
	no strict 'refs';
	no warnings 'once';
	*{"${callpkg}::$sym"} =
            !defined($type) ? \&{"${pkg}::$sym"} :
	    $type eq '&'    ? \&{"${pkg}::$sym"} :
	    $type eq '$'    ? \${"${pkg}::$sym"} :
	    $type eq '@'    ? \@{"${pkg}::$sym"} :
	    $type eq '%'    ? \%{"${pkg}::$sym"} :
	    $type eq '*'    ?  *{"${pkg}::$sym"} :
	    do { require Carp; Carp::croak("Can't export symbol: $type$sym") };
    }

    # Handle the special tied variables
    Text::CSV::Edit::OO::_tie_sheet_vars_to_pkg($callpkg, \@sheetvar_imports, 
                                                $Exporter::Debug);
  # copied from List::Util
  { my $caller = caller;
    # (RT88848) Touch the caller's $a and $b, to avoid the warning of
    #   Name "main::a" used only once: possible typo" warning
    no strict 'refs';
    ${"${caller}::a"} = ${"${caller}::a"};
    ${"${caller}::b"} = ${"${caller}::b"};
  }
}

#sub to_array(@)  { @_ > 1 ? @_ 
#                   : @_==0 ? ()
#                   : ref($_[0]) eq "ARRAY" ? @{$_[0]} 
#                   : ref($_[0]) eq "HASH"  ? @{ %{$_[0]} }   # key => value, ...
#                   : ($_[0]) 
#                 }
#sub to_aref(@)   { [ to_array(@_) ] }
#sub to_wanted(@) { goto &to_array if wantarray; goto &to_aref }
#sub to_hash(@)   { return $_[0] if @_==1 && ref($_[0]) eq "HASH";
#                   confess "Expecting key => value, pairs" unless (@_ % 2) == 0;
#                   return { to_array(@_) } 
#                 }

sub fmt_sheet($) {
  my $sheet = shift;
  return "undef" unless defined $sheet;
  my $s = $sheet->sheetname() || $sheet->data_source();
  while (length($s) > 30) { $s =~ s#^(?:\.\.\. )?.+/#... # or last; }
  return $s ? qsh($s) : "$sheet";
}

# Default options for new sheets created with the procedural API.
our $verbose;
our $debug;
our $silent;

# 
# %opts = proc_options(\@_)
#   Check that an even number of args were passed, and that keys are plausible
sub proc_options($) {
  my ($err, $optsaref) = &Text::CSV::Edit::OO::process_args_func;
  croak $err if $err;
  @$optsaref
}

sub _newsheet($$@) {
  my ($pkg, $caller_level_adj, $logthisnew, @rest) = @_;
  my %opts = proc_options(\@rest); # checks that args are in pairs, etc.
  
  if ($logthisnew) {
    # Set default new-sheet options (procedural API only)
    $opts{verbose}      = $verbose if defined($verbose) && ! exists $opts{verbose};
    $opts{debug}        = $debug   if defined($debug) && ! exists $opts{debug};
    $opts{silent}       = $silent  if defined($silent) && ! exists $opts{silent};
  }

  my $sheet = Text::CSV::Edit::OO->new(caller_level => 1+$caller_level_adj, 
                                       %opts);

  # Make the sheet the caller's "current" sheet for procedural API 
  $pkg2currsheet{$pkg} = $sheet;

  return $sheet;
}

# OO api "new" This is so users do not have to
# know about the :OO subclass, and can just write
#
#   my $obj = Text::CSV::Edit->new(...)
#
sub new {
  my $this = shift;
  my $class = ref($this) || $this;
  my %opts = proc_options(\@_);

  # Default verbose, silent, debug from the previous current sheet, if any
  if (my $sheet = $pkg2currsheet{scalar(caller)}) {
    foreach my $key (qw/verbose silent debug/) {
      $opts{$key} = $$sheet->{$key} if defined($$sheet->{$key});
    }
  }

  return Text::CSV::Edit::OO->new(caller_level => 1, %opts);
}

sub callmethod($@) {
  my $methname = shift;
  my $pkg = caller(1);

  my $sheet = $pkg2currsheet{$pkg};
  if (! $sheet) {
    $sheet = _newsheet($pkg,2,0);
  } else {
    if (@{$sheet->rows} > 0 && $methname eq "read_spreadsheet" 
         && ! $$sheet->{silent}) {
      carp "WARNING: $methname will over-write existing data",
           " (",$sheet->data_source,")\n(Set 'silent' to avoid this warning)\n";
    }
  }
  confess "bug" if ($$sheet->{caller_level} += 3) != 3;

  my ($result, @result);
  if (wantarray) {
    @result = eval { $sheet->${methname}(@_) };
  }
  elsif (defined wantarray) {
    $result = eval { $sheet->${methname}(@_) };
  } 
  else {
    eval { $sheet->${methname}(@_) };
  }

  if ($@) {
    $$sheet->{caller_level} = 0;
    die $@;
  }
  confess "bug" if $$sheet->{caller_level} != 3;
  $$sheet->{caller_level} -= 3; # should now be zero
  
  # Only callers' packages should ever have a "current sheet"
  confess "bug" if $pkg2currsheet{__PACKAGE__};

  wantarray ? @result : $result;
}

sub callmethod_checksheet($@) {
  my $pkg = caller(1);
  croak $_[0],": No sheet is defined for package $pkg\n" unless $pkg2currsheet{$pkg};
  goto &callmethod;
}

sub alias(@) { callmethod_checksheet("alias", @_) }
sub apply_all(&;@) { callmethod_checksheet("apply_all", @_) }
sub apply(&;@) { callmethod_checksheet("apply", @_) }
sub apply_exceptrx(&$;@) { callmethod_checksheet("apply_exceptrx", @_) }
sub apply_torx(&$;@) { callmethod_checksheet("apply_torx", @_) }
sub attributes(@) { callmethod("attributes", @_) }
sub autodetect_title_rx(@) { callmethod("autodetect_title_rx", @_) } #DEPRECATED
sub spectocxlist(@) { callmethod("spectocxlist", @_) }
sub data_source(;$) { callmethod("data_source", @_) }
sub delete_col($)  { goto &delete_cols; }
sub delete_cols(@) { callmethod_checksheet("delete_cols", @_) }
sub delete_row($)  { goto &delete_rows; }
sub delete_rows(@) { callmethod_checksheet("delete_rows", @_) }
#sub forget_title_rx() { callmethod_checksheet("forget_title_rx", @_) }
sub invert() { callmethod_checksheet("invert", @_) }
sub join_cols(&@) { goto &join_cols_sep; }
sub join_cols_sep($@) { callmethod_checksheet("join_cols", @_) }
sub move_col($$) { goto &move_cols }
sub move_cols($@) { callmethod_checksheet("move_cols", @_) }
sub new_col($$) { goto &new_cols }
sub new_cols($@) { callmethod("new_cols", @_) }
sub new_row(;$) { goto &new_rows; }
sub new_rows(;$$) { callmethod("new_rows", @_) }
sub only_cols(@) { callmethod_checksheet("only_cols", @_) }
sub options(@) { callmethod("options", @_) }
sub read_spreadsheet($;@) { callmethod("read_spreadsheet", @_) }
sub rename_cols(@) { callmethod_checksheet("rename_cols", @_) }
sub reverse_cols() { callmethod_checksheet("reverse_cols", @_) }
sub sort_rows(&) { callmethod_checksheet("sort_rows", @_) }
sub sheetname() { callmethod_checksheet("sheetname", @_) }
sub split_col(&$$$@) { callmethod_checksheet("split_col", @_) }
sub tie_column_vars(;@) { callmethod("tie_column_vars", @_) }
sub tied_varnames(;@) { callmethod("tied_varnames", @_) }
sub title_row() { callmethod_checksheet("title_row", @_) }
sub title_rx(;$@) { callmethod_checksheet("title_rx", @_) }
sub first_data_rx(;$) { callmethod_checksheet("first_data_rx", @_) }
sub last_data_rx(;$) { callmethod_checksheet("last_data_rx", @_) }
sub unalias(@) { callmethod_checksheet("unalias", @_) }
sub untie_column_vars() { callmethod("untie_column_vars", @_) }
sub write_csv(*;@) { callmethod_checksheet("write_csv", @_) }
sub write_spreadsheet(*;@) { callmethod_checksheet("write_spreadsheet", @_) }
sub write_fixedwidth(*;$) { callmethod_checksheet("write_fixedwidth", @_) }

# Return the current "active sheet" for a specified package and,
# if the third argument is passed, change it to another sheet 
# (or to "no active sheet" if undef is passed).
#
# If $log_caller_level is defined, then log the indicated ancestor sub's call
# if either sheet has 'verbose' enabled.
#
sub _internal_package_active_sheet($$;@) {
  my ($pkg, $log_caller_level, $newsheet) = @_;

  my $previous = $pkg2currsheet{$pkg};
  my $verbose = ($previous && $$previous->{verbose}) ||
                ($newsheet && $$newsheet->{verbose}) ;

  if ($verbose && defined($log_caller_level)) {
    warn Text::CSV::Edit::OO::logfunc($log_caller_level, 0, 
                                (@_ > 2 ? (\(fmt_sheet($newsheet)." ")) : ()),
                                \"(pkg $pkg) ",
                                \(u($previous) eq u($newsheet) 
                                   ? "[no change]"
                                   : "[returning ".fmt_sheet($previous)."]")
                                      );
  }
  if (@_ > 2) {
    if (defined $newsheet) {
      $pkg2currsheet{$pkg} = $newsheet ;
    } else { 
      delete $pkg2currsheet{$pkg} ;
    }
  }
  return $previous
}

sub package_active_sheet($;$) { # called directly by user code
  my ($pkg) = @_;
  _internal_package_active_sheet($pkg, 1, @_[1..$#_]);
}

# Retrieve the sheet currently accessed by the procedural API & tied globals
# in the caller's package (each package is independent).
# If an argument is passed, change the sheet to the specified sheet.
#
# Always returns the previous sheet (or undef)
sub sheet(;$) {
  _internal_package_active_sheet(caller, 1, @_)
}

# FUNCTION to tie variables to sheet internal data.
#
# !! This should NOT be called by users of the procedural interface !!
# (they should just import @row etc. from this package).
#
# Rather, this is for object-oriented users who can say
#    Text::CSV::Edit::tie_sheet_vars(['@rows','@myrowsvar'], ...)
# and not be aware of the ::OO package
#
sub tie_sheet_vars(@) {
  # The goto is necessary so 'caller' will give the user's package.
  goto &Text::CSV::Edit::OO::tie_sheet_vars;
}

# FUNCTION to produce the "automatic alias" identifier for an arbitrary title
sub title2ident($) {
  goto &Text::CSV::Edit::OO::title2ident;
}

# Non-OO api: Explicitly create a new sheet, optionally specifying options
# (possibly including the initial content).
# All the regular functions automatically create an empty sheet if no sheet
# exists, so this is only really needed when using more than one sheet,
# or if you want to initialize a sheet from data in memory.
sub new_sheet(@) {
  my ($pkg, $fname, $line) = caller;
  my $data_source = "(new_sheet called at ${fname}:$line)";
  return _newsheet($pkg, 1, 1, data_source => $data_source, @_);
}

# logmsg() - Concatenate strings to form a "log message", 
#   prefixed with a description of a sheet and optionally specific row,
#   and suffixed by a final \n if needed.
#
# A "focus" sheet and row, if any, are determined as follows:
#
#   If the first argument is a sheet object, [sheet_object],
#   [sheet_object, rx], or [sheet_object, undef] then the indicated
#   sheet and (optionally) row are used.
#
#   Otherwise the first arg is not special and is included in the message.
#
#   If no sheet is identified above, then the caller's package active
#   sheet is used, if any.   
#
#   If still no sheet is identified, then the sheet of the innermost apply
#   currently executing (anywhere up the stack) is used, if any; this sheet
#   is internally saved in a global by the apply* methods.
#
#   If a sheet is identified but no specific rx specified, then the
#   "current row" of an active apply on that sheet is used, if any.
#
# If a focus sheet or sheet & row were identified, then the caller-supplied 
# message is prefixed by "(<description>):" or "(row <num> <description>):"
# where <description> comes from:
# 
#   1) If the sheet attribute {logmsg_pfx_gen} is defined to a subref,
#      the sub is called and all returned items other than undef are 
#      concatenated (any undefs in the returned list are ignored); otherwise
#
#   2) The "sheetname" property is used, if defined; otherwise
#
#   3) the "data_source" property is used, which defaults to the name of the 
#      spreadsheet read by read_spreadsheet().
#
sub _default_pfx_gen($$) {
  my ($sheet, $rx) = @_;
  confess "bug" unless ref($sheet) =~ /^Text::CSV::Edit\b/;
  ($sheet->sheetname() || $sheet->data_source())
}
sub logmsg(@) {
  my ($sheet, $rx);
  if (@_ > 0 && ref($_[0])) {
    if (ref($_[0]) =~ /^Text::CSV::Edit\b/) {
      #warn "###L1 first arg is a sheet\n";
      $sheet = shift;
    }
    elsif (ref($_[0]) eq "ARRAY" 
           && @{ $_[0] } == 2 
           && ref($_[0]->[0])."" =~ /^Text::CSV::Edit\b/) {
      #warn "###L2 first arg is [sheet,something]\n";
      ($sheet, $rx) = @{ shift @_ };
    }
  }
  if (! defined $sheet) {
    $sheet = _internal_package_active_sheet(caller,undef);
  }
  if (! defined $sheet) {
    $sheet = $Text::CSV::Edit::OO::_inner_apply_sheet; 
  }
  if (! defined $rx) {
    $rx = eval{ $sheet->rx() } if defined($sheet);
  }
  my @prefix;
  if (defined $sheet) {
    my $pfxgen = $sheet->attributes->{logmsg_pfx_gen} // \&_default_pfx_gen;
    push @prefix, "(", (defined($rx) ? "Row ".($rx+1)." " : "");
    push @prefix, grep{defined} &$pfxgen($sheet, $rx);
    push @prefix, "): ";
  }
  my $suffix = (@_ > 0 && $_[-1] =~ /\n\z/s ? "" : "\n");
  return join "", @prefix, @_, $suffix;
}

1;
__END__

=pod

=encoding utf8

=head1 NAME

Text::Csv::Edit - tools to manipulate spreadsheet data

=head1 NON-OO SYNOPSIS

  use Text::CSV::Edit qw(:DEFAULT :STDVARS logmsg);

  my $previous = options verbose => 1;  # Log calls to stderr

  # Read spreadsheet with columns "Account Number", "Customer Name",
  # "Email" and "Income"
  read_spreadsheet "mailing_list.xls!sheet name"; 
  title_rx "auto";
  alias Name => qr/customer/i;  # matches "Customer Name" 

  # Tie globals corresponding to column titles or aliases for titles
  # which exist now or in the future
  tie_column_vars "Account_Number", "Name", qr/^inc/i, "FName", "LName";

  our $Name;            # 'Name' is an explicit alias 
  our $Account_Number;  # Auto-generated alias for title "Account Number"
  our $Income;          # 'Income' is an actual title 
  our $Email;           # 'Email' is an actual title 

  our ($FName, $LName); # Not yet valid

  # Split "Customer Name" into new "FName" and "LName" columns
  new_cols '>$', "FName", "LName";
  apply { 
    ($FName,$LName) =~ ($Name =~ /(.*) (.*)/) 
  };

  # Print the last 3 records
  apply_torx {
    print "A/N=$Account_Number Fname=$FName LName=$LName Income=$Income\n";
  } [$#rows-2, $#rows];

  # Simple mail-merge
  use POSIX qw(strftime);
  apply {
    if ($Income <= 0) {
      warn logmsg("Invalid Income '$Income'");
      return
    }
    return if $Income < 100000;  # not in our audience
    open SENDMAIL, "|sendmail -t -oi" || die "pipe:$!";
    print SENDMAIL "To: $FName $LName <$Email>\n";
    print SENDMAIL strftime("Date: %a, %d %b %y %T %z\n", localtime(time));
    print SENDMAIL <<EOF ;
  From: sales\@example.com
  Subject: Help for the 1%

  Dear $FName,
    If you have disposable income, we can help with that.
  Sincerely,
  Your investment advisor.
  EOF
    close SENDMAIL || die "sendmail failed ($?)\n";
  };

  # List Names and Incomes.  
  print "--- database ---\n";
  apply {
    print "@_\n";
  } qw(Name Income); # specified columns are bound to @_

  # Use multiple sheets
  my $previous_sheet = sheet(undef); # current sheet becomes undefined
  read_spreadsheet "file2.csv";      # auto-creates new sheet
  my $sheet2 = sheet();              # remember (the new) current sheet
  title_rx 2;                        # (two header rows before titles)
  first_data_rx 5;                   # (another two header rows after titles);
  tie_column_vars;                   # tie all possible column variables
  our ($Foo, $Bar);
  print "$Foo $Bar $Income\n"; # all refer to second sheet
  sheet($previous_sheet);
  print "$FName $LName $Income\n"; # variables now refer to the original sheet

  # Create a sheet from data in memory
  my $sheet3 = 
     new_sheet
        data_source => "my own data",
        rows => [ ["Full Name",  "Address",         "City",   "State", "Zip"  ],
                  ["Joe Smith",  "123 Main St",     "Boston", "CA",    "12345"],
                  ["Mary Jones", "999 Olive Drive", "Fenton", "OH", "   67890"],
                ]
        ;
  title_rx 0;
  ...
  
  # import & tie sheet-control variables but use non-standard names
  Text::CSV::Edit::tie_sheet_vars(['@rows','@myrows'], ['$num_cols', '$mync']);
  our (@myrowsvar, $mync)
  print "There are $mync columns and ", ($#myrows+1), " rows.\n";

=head1 OO SYNOPSIS

  my $sheet = Text::CSV::Edit->new();

  $sheet->attributes->{MyKey} = $MyValue;

  $sheet->options(verbose => 1);
  
  $sheet->read_spreadsheet("mailing_list.xls!sheet name");
  $sheet->title_rx(0);
  $sheet->alias( Name => qr/customer.*name/i );

  print "--- database ---\n";
  $sheet->apply( sub{
    # Sheet objects behave as a hash for cells in the 'current' row
    print "Name:", $sheet->{Name}, "  Income:", $sheet->{Income}, "\n";
  });
    
  # Randomly access rows.
  # Sheet objects also behave as an array of "row"s, each of which behaves
  # as either a hash indexed by title, ABC name, etc. or as an array of cells.
  
  print "Row 42: Name is ",     $sheet->[41]->{Name}, "\n";
  print "Row 42, Column 3 is ", $sheet->[41]->[2],    "\n";
  
  # "Join" two spreadsheets
  $sheet->apply( sub{
    print "$sheet->{"Customer Name} has income $sheet->{Income}\n";
    $sheet2->apply( sub{
      if ($sheet2->{Name} eq $sheet->{Name}) {
        print "$sheet->{Name} also has $sheet2->{Other_Stuff}\n";
      }
    });
  });

  # Intermix OO and procedural APIs 
  sheet($sheet);            # make $sheet be the package "current sheet"
                            # now the non--OO api works as described above

=head1 DESCRIPTION

This package allows easy manipulation of spreadsheet data.
Columns may be accessed by title, optionally using tied variables.

The usual paradigm is to iterate over the rows applying a function 
to each, vaguely inspired by 'sed' and 'awk' (see C<apply> below).
Random access is also supported.

Sheet-level operators allow
rows and columns to be created, deleted, or re-arranged.

A procedural interface is described first, followed by the object-oriented API.
The procedural API allows cells to be accessed as $NAME using tied global variables.

=head1 PROCEDURAL (NON-OO) DESCRIPTION

=head2 THE "CURRENT SHEET" for the procedural API

Functions implicitly operate on the "current sheet" of the calling package.  
There is a separte "current sheet" (initially none) for each package.

A new empty sheet is created by any operation if the calling package
does not have a "current sheet". 

The "current sheet" may be saved and later restored (or unset), allowing an
application to switch among data sets. The OO API provides direct
access to multiple sheets at once, but with less sugar.

=head2 $curr_sheet = sheet

=head2 $prev_sheet = sheet($another_sheet)

=head2 $prev_sheet = sheet(undef)

=head2 $prev_sheet = package_active_sheet($package_name)

=head2 $prev_sheet = package_active_sheet($package_name, $another_sheet)

=head2 $prev_sheet = package_active_sheet($package_name, undef)

C<sheet> retrieves the current "sheet" object used by the procedural API
and optionally changes it to something else.

Changing the current sheet implicitly changes what is referenced by 
sheet variables (see STANDARD SHEET VARIABLES) and tied column globals.  

Setting C<undef> reverts the package to having no current sheet.
If the application does not save a reference to the previous sheet,
the sheet object is deleted.

C<package_active_sheet> is similar but operates on the "current sheet"
of an arbitrary package.  This is useful in library modules which need 
to access the "current sheet" of their caller.

=head1 STANDARD SHEET VARIABLES for the Procedural API

These magically access whichever sheet is the "current sheet" 
used by your package.

These are not imported by default, but may be imported with the C<:STDVARS> 
tag (also use C<:DEFAULT> to retain the default API functions).

=over

=item @rows, @linenums, and $num_cols

@rows contains the spreadsheet data.  It is an array of arrays; each element is 
a ref to an array of cells for one row.

@linenums is valid if the data came from a CSV file; each element contains the 
first line number of the corresponding row (a row may contain multiple
lines due to newlines embedded within cells).

$num_cols holds the number of columns in the widest input row (short rows are
padded with empty cells when read so that all rows have the same number 
of columns in memory).

=item $title_rx and $title_row

If a title row has been designated (see C<title_rx()>), then C<$title_rx> contains it's 0-based
row index and C<$title_row> is an alias for C<$rows[$title_rx]>.

=item $row, %rowhash (or %row), $rx and $linenum (valid during C<apply>)

When iterating over rows with C<apply>, these variables
refer to the current row.  

C<$rx> is the current row's index.

C<$row> is a reference to an array containing the cells in the current row 
(the same as S< C<< $rows[$rx] >> >).

C<%rowhash> is a virtual hash which maps "column specifiers"
to the corresponding cell in the I<current> row.  
C<%row> is a deprecated synonym.

The following are
all equivalent, and access the column with title "FIRST NAME":

    $rowhash{"FIRST NAME"}            # using %rowhash with title as key
    $rowhash{FIRST_NAME}              # using automatic alias
    $row{FIRST_NAME}                  # same using deprecated %row
    $row->[ $colx{"FIRST NAME}} ]     # $row is a ref to array of cells
    $rows[$rx]->[ $colx{FIRST_NAME} ] # @rows contains refs to arrays of cells

See "COLSPECs" for a description of the possible keys.

See "GLOBAL COLUMN VARIABLES" for a possibly-more-friendly alternative.

=item $first_data_rx and $last_data_rx

Optional limits on the range of rows visited by C<apply()>
or sorted by C<sort_rows()>.  

=item %colx (column key => column index)

C<< %colx >> maps actual column titles, aliases, automatic aliases
and spreadsheet-letter codes (A,B,...Z, AA etc.) to zero-based
column indices.   All are present unless collisions occur (see below).

B<AUTOMATIC ALIASES> are Perl I<identifiers> derived from column titles by 
replacing non-word characters with underscores and prepending 
an underscore if necessary.  For example:

    Title             Automatic Alias

    "First Name"      First_Name
    "First & Last"    First___Last
    "+sizes"          _sizes
    "1000s"           _1000s  (underscore avoids leading digit)
    "Address"         Address (no change needed)

Aliases (both automatic and user-defined) are valid identifiers, 
so can be used as the names of tied global column variables (described later)
and as bareword keys to C<%colx> and related OO interfaces,

CONFLICT RESOLUTION

A conflict occurs when a column key potentially refers to multiple 
columns. For example, the standard spreadsheet column code "A" can
not be used to refer to the first column if another column has an actual title "A".
Warnings are printed about conflicts unless the C<silent> option is true (see C<options>).

=over

The B<special names "^" and "$"> always refer to the first and last column.

B<Actual Titles> may always be used, unless they are "^" or "$".

B<User Aliases> (defined using C<alias>) are always valid.  This is enforced by
throwing an exception if an alias is defined which conflicts with an
actual title.

B<ABC column codes> ("A", "B", etc.) and B<numeric column indicies> 
are available as %colx keys only if a same-named B<Title> does not exist.
If a conflicting Title exists, then the key always refers to the 
column containing that Title.

B<Automatic Aliases> (identifiers constructed from non-identifier Titles)
are available only if they don't conflict with the above.

=back

=item %colx_desc (column key => "debugging info")

=back

=head1 COLSPECs (COLUMN SPECIFIERS)

Arguments which specify columns may be any of the following:

=over

=item (1) '^' or '$' (means first or last column, respectively)

=item (2) "column title" (if a title row is defined)

=item (3) alias (user-defined or automatic)

=item (4) spreadsheet letter code ("A","B", etc.) 

=item (5) 0-based numeric column index

=back

(Letter codes and 0-based indicies may not be used as such if they
are identical to a column title).

Column positions always refer to data before the command is
executed.  This is relevant for commands which re-number columns,
such as C<delete_col> and C<move_col>.

=head1 GLOBAL COLUMN VARIABLES

Package variables can be used to access cells,
for example C<$FName> and C<$Email> in the SYNOPSIS above.

I<tie> is used to bind these variables to the corresponding
cell in the current row during execution of C<apply>;
reading or writing these variables directly access the appropriate cells.

See C<tie_column_vars>.

=head1 FUNCTIONS

=head2 read_spreadsheet CSVFILEPATH [,OPTIONS...]

=head2 read_spreadsheet SPREADSHEETPATH, sheet => "SHEETNAME" [,OPTIONS]

=head2 read_spreadsheet "SPREADSHEETPATH!SHEETNAME" [,OPTIONS]

Replace any existing data with the contents of the given file.  
Sets C<data_source>.
The file may be a .csv or any format supported by Libre Office or gnumeric.

OPTIONS:

=over 6

=item sheet => SHEETNAME

Specify which sheet in a workbook (i.e. spreadsheet file) to read.  
Defaults to the "last used", i.e. "active" sheet when the spreadsheet was saved.

Alternatively, the sheet name may be appended to the input path after '!' as shown in the example.

=item silent => bool

=item verbose => bool

=item debug => bool

=item Other C<< key => value >> pairs override details of CSV parsing.  

See Text::CSV::Spreadsheet for details on these rarely-needed options, 
including file encoding (which defaults to UTF-8).

=back

If a spreadsheet (.xls, etc.) then only the indicated "sheet" 
is read (or if no sheet is specified, then the "active" sheet when saved).
If the sheet name is not given seprately then it may be appended to the 
path after a '!', e.g. "/path/to/file.xls!sheet name".

Due to bugs in Libre/Open Office, spreadsheet files can not
be read if LO/OO is currently running, even
for unrelated purposes.  This problem does not occur with .csv files.

=head2 new_sheet  

[procedural API only] 
Create a new empty sheet.  Returns the sheet object.
Rarely used because a new sheet is automatically created by any operation 
if the package has no current sheet.  

See also the C<new()> method in the OO API.

=head2 new_sheet {rows => [...], linenums=>[...], data_source=>"..."}

[procedural API only] 
Create a new sheet and initialize it with data already in memory.  
The referenced data must not be modified directly while associated 
with a sheet.

=head2 alias IDENT => COLSPEC, ... ;

=head2 alias IDENT => qr/regex/, ... ;

Create alternate names for specified columns.  
Multiple pairs may be given.  
Each IDENT must be a valid Perl identifier which does not correspond 
to the the title of any other column (but which may be identical to
the title in the same column).

Regular expressions must match exactly one column title.  

Returns the column index (or indices).

Afterwards, C<$colx{IDENT}> will contain the index of the column, 
I<which will be automatically adjusted later if columns are inserted 
or deleted.>

=head2 unalias IDENT, ... ;

Removes alias definition(s)

=head2 spectocxlist COLSPEC or qr/regex/, ... ;

Returns the 0-based indicies of the indicated colomn(s).
Throws an exception if there is no such column.
A regex may match multiple columns.  
See also C<%colx>.

=head2 title_rx ROWINDEX ;

Specify which row contains titles (first is index 0).
Pass C<undef> to revert to having no title row.  

=head2 title_rx "auto" OPTIONS... ;

Auto-detect the title row.  OPTIONS may include

  min_rx   => NUM,   # first rx which may contain the title row.
  max_rx   => NUM,   # maximum rx which may contain the title row.
  first_cx => NUM,   # first column ix which must contain a valid title
  last_cx  => NUM,   # last column ix which must contain a valid title
  required => COLSPEC or [COLSPEC,...]  # required title(s)

The first row with non-empty titles in all positions, 
and which include the C<required> title(s), if any, is used.

An exception is thrown if a plausible title row can not be found.

See also C<autodetect_title_rx>.

=head2 title_rx ;

With no arguments, returns the current value (same value as C<$title_rx>).

=head2 tie_column_vars [{OPTIONS},] "varname", ...

=head2 tie_column_vars [{OPTIONS}] ;

Create tied global scalar variables for use during C<apply>.

Each variable corresponds to a column, and reading or writing
it accesses the corresponding cell in the row being visited during C<apply>.
Usually you must also declare these variables with C<our $name>, 
but see "Use in BEGIN Blocks" for exceptions.

The arguments name variables to tie (with or without the '$' sigl).
The identifiers must either be derived from titles by replacing spaces 
and punctuation with underscrores (see "AUTOMATIC ALIASES"), or
user-defined alias names (see "alias"), or spreadsheet column letters
like "A", "B" etc.  See "CONFLICT RESOLUTION" about name clashes.

If no names are specified, then I<all possible variables> are tied,
i.e. corresponding to all alises, titles, 
and non-conflicting spreadsheet column letters in the 
current sheet.  This is convenient but B<insecure> because malicious spreadsheet 
titles could clobber unintended globals; however see "Use in BEGIN Blocks".

Multiple calls accumulate, including with different sheets.  

Variable bindings are dynamically evaluated duing each access based
on the current sheet at the time of access.  
Example: If different sheets contain a column with the same title (possibly
in different positions), then a single tied variable for that title
will access the appropriate column in the current sheet.
Referencing a variable with no valid binding in the current sheet
has unspecified effects.

=head3 {OPTIONS}

If the first argument is a hashref, it may specify the following: 

=over

=item package => "pkgname"

Tie variables in the specified package instead of the caller's package.

=item verbose => bool

=item debug => bool

Print trace messages.

=back

=head3 Use in BEGIN Blocks

C<tie_column_vars> imports tied scalars into the caller's
package, so they do not need to be pre-declared when the call is
within a C<BEGIN{  }> block, or in some other way occurs before
the compiler sees the first mention of a tied variable.
C<Text::CSV::Edit::Preload> makes use of this.

Also, when called at compile time, C<tie_column_vars> checks that
variables to be tied do not already exist, eliminating the security risk 
described above when names are not given explicitly.  
An exception is thrown if names were specified explicitly, otherwise
a warning is printed for clashing names and execution continues.
You I<must not> pre-declare variables with this usage.

Note: The security check actually looks for a Perl symbol table 
entry ("glob"), so the check will fail if any same-named object
exists, such as a filehandle, array, hash, etc.

=head2 autodetect_title_rx OPTIONS ;

(deprecated)
Arrange to auto-detect I<title_rx> the first time titles are needed,
for example when calling C<alias> with a non-absolute 
COLSPEC or referencing a variable previously passed to C<tie_column_vars>.

OPTIONS are as with 'title_rx "auto"' except that C<required> may not
be specified.

=head2 apply {code} COLSPEC* ;

=head2 apply_all {code} COLSPEC* ;

=head2 apply_torx {code} RX-OR-RXLIST COLSPEC*

=head2 apply_exceptrx {code} RX-OR-RXLIST COLSPEC*

Execute the specified code block (or referenced sub) once for each row.

While executing the code block, tied column variables and
the sheet variables C<$row>, C<%rowhash>, C<$rx> and C<$linenum> 
(and corresponding OO methods) will refer to the row being visited.

If COLSPEC(s) are specified, then the indicated columns are bound to @_ in the order 
given, and the first of them is also bound to $_.   Reading or writing $_ or elements
of @_ access the corresponding cells.

C<apply> normally visits all rows which follow the title row (or all rows
if there is no title row). 
If B<first_data_rx> and B<last_data_rx> are defined, then they
further limit the range visited.  

C<apply_all> unconditionally visits every row, including any title row.

C<apply_torx> or C<apply_exceptrx> visits exactly the specified rows
or all except the specified rows (including any title row).
RX-OR-RXLIST may be either a single scaler row index or a [list of rx];

An apply sub may safely insert or delete rows; no rows will be visited
more than once, and none skipped except for new rows inserted at positions
before the currently-being-visited row.

An apply sub may change the "current sheet" (see C<sheet()>),
and then global variables will refer to the other sheet and
any C<apply> active for that sheet.  It should restore
the original sheet before returning (Guard::scope_guard may be useful for this).
Nested and recursive C<apply>s
are allowed.

=head2 delete_col COLSPEC ;

=head2 delete_cols COLSPEC+ ;

The indicated columns are removed.  Remaining title alias bindings
are adjusted to track any shifted columns.

=head2 only_cols COLSPEC+ ;

All columns I<except> the specified columns are deleted.

=head2 move_cols POSITION, SOURCES... ;

=head2 move_col  POSITION, SOURCE ;

Relocate the indicated column(s) (C<SOURCES>) so they are adjacent, in 
the order specified, starting at the position C<POSITION>.

POSITION may be ">COLSPEC" to place moved column(s) 
immediately after the indicated column;
or POSITION may directly specify the destination column
using an unadorned COLSPEC.

Non-absolute COLSPECs indicate the initial positions of the referenced columns.

=head2 new_col  POSITION, newtitle ;

=head2 new_cols POSITION, newtitles... ;

One or more columns are created starting at a position
specified the same way as in C<move_cols> (existing columns
at or after that position are moved rightward).

POSITION may be ">$" to place new column(s) immediately after the last existing column.

New titles must be listed for every new column; if there is no title
row, specify "" or undef.

Returns the new column index or indices.

=head2 split_col {code} COLSPEC, POSITION, newtitles... ;

New columns are created starting at POSITION as with C<new_cols>, 
and populated with data from column COLSPEC.

C<{code}> is called for each row with $_ bound to the cell at COLSPEC
and @_ bound to cell(s) in the new column(s).  It is up to your code to 
read the old column ($_) and write into the new columns (@_).

The old column is left as-is (not deleted).

=head2 sort_rows {rx cmp function}

=head2 sort_rows {rx cmp function} $first_rx, $last_rx

Re-order a set of rows.  If no range is specified, then the range is the
same as for C<apply> (namely: All rows after the title row unless
limited by B<first_data_rx> .. B<last_data_rx>).

The comparison function is called passing row objects in globals $a and $b
similar to Perl's C<sort>.  In addition, the row indicies of rows being
compared are passed as arguments in @_.

Rows are not actually moved until after all comparisons have finished,
so row index values are always the initial positions.

Returns a list of the previous row indicies of all rows in the sheet.

For example:

    # Sort current sheet on the "LastName" column
    sort_rows { my ($rxa, $rxb) = @_; 
                $rows[$rxa]->[$colx{LastName}] cmp $rows[$rxb]->[$colx{LastName}]
              };

or more efficiently,

    my $cx = $colx{LastName};
    sort_rows { $rows[$_[0]]->[$cx] cmp $rows[$_[1]]->[$cx] };

or using the OO interface (and the fact that sheet objects act as virtual arrays of virtual hashes):

    $sheet->sort_rows( sub{ my ($rx1, $rx2) = @_; 
                            $sheet->[$rx1]->{LastName} cmp $sheet->[$rx2]->{LastName}
                          } );

=head2 rename_cols COLSPEC, "new title", ... ;

Multiple pairs may be given.  Title cell(s) are updated as indicated.

Existing aliases are I<not> affected, i.e., they continue to refer to the
same columns as before.


=head2 join_cols_sep STRING_OR_SUBREF COLSPEC+ ;

=head2 join_cols {code} COLSPEC+ ;

The specified columns are combined into the first-specified column and the other
columns are deleted.

The first argument of C<join_cols> may be a {code} block or subref;
The first argument of C<join_cols_sep> may be a fixed separator 
string or a subref.

If a string is specified it is used to join column content 
together, except that the surviving column's title (if any) is not changed.

If a {code} block or sub ref is specified,
it is executed once for each row (including title row) with
$_ bound to the first-named column, i.e. the surviving column,
and @_ bound to all named columns in the order given.
It is up to your code to combine the data by reading
@_ and writing $_ (or, equivalently, by writing $_[0]).

=head2 reverse_cols

The order of the columns is reversed.

=head2 invert

"Rotate and flip" the table.  Cells A1,B1,C1 etc. become A1,A2,A3 etc.
Any title_rx is forgotten.

=head2 new_row

=head2 new_row $rowx

=head2 new_rows $rowx [,$count] ;

Insert one or more empty rows at the indicated position
(default: at end).  C<$rowx>, if specified, is either a 0-based offset 
for the new row or '$' to add the new row(s) at the end.
Returns the index of the first new row.

=head2 delete_rows $rowx,... ;

=head2 delete_rows 'LAST',... ;

=head2 delete_rows '$',... ;

The indicated data rows are deleted.  C<$rowx> is a zero-based row index
or a special token (either '$' or "LAST") to indicate the last row.  
Any number of rows may be deleted in a single command, listed individually.

=head2 Text::CSV::Edit::logmsg [FOCUSARG,] string, string, ...

(not exported by default)

Concatenate strings to form a "log message", 
prefixed with a description of a sheet and optionally specific row,
and suffixed by a final \n if needed.

If the first argument is a sheet object, [sheetobj], or [sheetobj,rowindex] 
then it specifies the sheet or sheet & row to describe; otherwise the
first argument is not special (but is just the first message string).
If a sheet or row is not specified, then the "current" sheet for the calling
package is used, and the "current" row in an active apply.

The way a sheet/row is formatted may be customized with a {logmsg_pfx_gen}
attribute call-back.  For details, see comments in the source.

=head2 write_csv CSVFILEPATH [,OPTIONS]

=head2 write_csv *FILEHANDLE [,OPTIONS]

=head2 write_csv $filehandle [,OPTIONS]

Write the current data to the indicated path or open file handle as a CSV text file.
The default encoding is UTF-8 or, if C<read_spreadsheet> was most-recently
used to read a csv file, the encoding used then.

OPTIONS is a list of key => value pairs 
(a deprecated alternative is a {hashref} with similar information).

OPTIONS may include any of the options to Text::CSV, but usually none
need to be specified becuase we supply sane and reasonable defaults.

=over 6

=item col_formats => [ LIST ]

Elements of LIST may be "" (Standard), "Text", "MM/DD/YY", "DD/MM/YY", or "YY/MM/DD"
to indicate the format of the corresponding column.  The meaning of "Standard" is not
well documented but appears to mean "General Number" in most cases.   For details, see
"Format Codes" in L<this old Open Office documentation|https://wiki.openoffice.org/wiki/Documentation/DevGuide/Spreadsheets/Filter_Options#Filter_Options_for_the_CSV_Filter>.

=item silent => bool

=item verbose => bool

=item debug => bool

=back

=head2 write_spreadsheet OUTPUTPATH OPTIONS...

Write the current data to a spreadsheet (.ods, .xlsx, etc.) by first writing to a
temporary CSV file and then importing that file into a new spreadsheet.

OPTIONS are a list of key => value pairs:

=over 6

=item col_formats => [ LIST ]

Elements of LIST may be "" (Standard), "Text", "MM/DD/YY", "DD/MM/YY", or "YY/MM/DD"
to indicate the format of the corresponding column.  The meaning of "Standard" is not
well documented but appears to mean "General Number" in most cases.   For details, see
"Format Codes" in L<this old Open Office documentation|https://wiki.openoffice.org/wiki/Documentation/DevGuide/Spreadsheets/Filter_Options#Filter_Options_for_the_CSV_Filter>.

=item silent => bool

=item verbose => bool

=item debug => bool

=back


=head2 options NAME => EXPR, ... ;  

=head2 options NAME ;

Set or retrieve miscellaneous sheet options.   
When setting, the previous value of
the last option specified is returned.  The only options currently defined
are I<silent>, I<verbose> and I<debug>.

=head2 $hash = attributes ;

Returns a reference to a hash which you may use to store arbitrary data
in the sheet object in memory.

=head1 OO DESCRIPTION (OBJECT-ORIENTED INTERFACE)

=head2 Text::CSV::Edit->new(OPTIONS...)

=head2 Text::CSV::Edit->new(clone => $existing_sheet)

=head2 Text::CSV::Edit->new(rows => [rowref,rowref,...], linenums => [...], data_source => "where this came from");

=head2 Text::CSV::Edit->new(num_cols => $number)  # no initial content

          
Creates a new "sheet" object.

=head2 METHODS

Sheet objects have methods named identically to all the functions
described previously (except for C<sheet()> and C<new_sheet()>, 
and C<logmsg>) which are only procedural-API functions).

Note that Perl requires parenthesis around all method arguments.

The following additional methods are available:

=head2 $sheet->rows() ;             # Returns aref to rows, analogous to to \@rows

=head2 $sheet->linenums() ;         # Analogous to \@linenums

=head2 $sheet->data_source();       # "description of sheet" (e.g. path read)

=head2 $sheet->sheetname();         # valid if input was a spreadsheet, else undef

=head2 $sheet->num_cols() ;         # Analogous to $num_cols

=head2 $sheet->colx() ;             # Analogous to \%colx

=head2 $sheet->colx_desc() ;        # Analogous to \%colx_desc

=head2 $sheet->title_row() ;        # Analogous to $title_row

=head2 $sheet->rx() ;               # Current row index in apply, analogous to to $rx

=head2 $sheet->current_row();       # Analogous to to $row

=head2 $sheet->rowhash() ;          # Analogous to to \%rowhash

=head2 $sheet->linenum() ;          # Analogous to to $linenum

=head2 $sheet->title_rx() ;         # Analogous to to $title_rx

=head2 $sheet->title_rx(rxvalue) ;  # (Re-)set the title row index

=head2 $sheet->get(rx,ident) ;      # Analogous to to $rows[rx]->[$colx{ident}]

=head2 $sheet->set(rx,ident,value); # Analogous to to $rows[rx]->[$colx{ident}] = value

=head2
       
=head1 SEE ALSO

Text::CSV::Edit::Preload

=head1 BUGS

Auto-detection of titles is in flux.  
This documentation should be updated to reflect how it currently works.
       
=head1 THREAD SAFETY

Unknown, and probably not worth the trouble to find out.
The author wonders whether tied variables are compatible with
the implementation of threads::shared. 
Even the OO API uses tied variables (for the magical row objects 
which behave as a hash).

=head1 AUTHOR

Copyright © Jim Avera 2012-2022.  Released into the Public Domain
by the copyright owner (jim.avera at gmail).
The author requests that this attribution be retained in copies 
or derivatives.

=cut

#!/usr/bin/perl
# Text::CSV::Edit Tester
use strict; use warnings  FATAL => 'all';; use 5.010;
use open IO => ':locale';
use open ':std' => ':locale';  # Make STDOUT/ERR match terminal
use utf8;
our @ISA;

# TODO: Test join_cols & join_cols_sep

use Carp;
use Vis;
sub verif_no_internals_mentioned($) {
  local $_ = shift;
  return 
    if $Carp::Verbose;  # carp becomes cluck with full traceback
  my $msg;
  # Ignore object refs 'Package::REF=(hexaddr)'
  # Ignore anonymous file handles '\*{"Package:"\$fh"}'
  if (/(?<!\\\*\{")Text::CSV(?!::[\w:]*(?:=REF|::\\\$))/s) { 
    $msg = "Log msg or traceback mentions internal package"
  }
  elsif ($@ =~ /\w+\.pm/s) {
    $msg = "Log msg or traceback mentions .pm file (presumably not user code)"
  }
  if ($msg) {
    my $posn = $+[0]; # offset of end of match
    my $start = rindex($_,"\n",$posn) + 1; 
    my $end = index($_,"\n",$posn); $end=length($_) if $end == -1;
    croak $msg, ":\n«", substr($_,$start,$end-$start), "»\n";
  }
}

our ($debug, $silent);
BEGIN {
  select STDERR; $|=1; select STDOUT; $|=1;
  $silent = 1;
  my $internal;
  while (@ARGV) {
    if ($ARGV[0] =~ /^(-d|--debug)/) { $debug=1; $silent=0; shift @ARGV }
    elsif ($ARGV[0] =~ /^(-s|--silent)/) { $silent=1; shift @ARGV }
    elsif ($ARGV[0] =~ /^(--no-silent)/) { $silent=0; shift @ARGV }
    elsif ($ARGV[0] =~ /^--internal/) { $internal=1; shift @ARGV }
    elsif ($ARGV[0] =~ /^--fail/) { die "fake failure" }
    else { die "$0: Unknown option: $ARGV[0]" }
  }
  if (!$internal) {
    use Test::More;
    plan tests => $debug ? 1 : 2;
    use Capture::Tiny qw/capture_merged tee_merged/;
    my @rerun_cmd = ($^X, $Carp::Verbose ? ("-MCarp=verbose"):(), $0);
    if (!$debug) {
      my ($output,$wstat) = tee_merged { system @rerun_cmd, "--internal" };
      ok(($wstat==0 || diag(sprintf "** WAIT STATUS = 0x%04X\n", $wstat)
            and
          $output eq "" || diag("Expected no output (w/o verbose or debug)"))
         , "The whole shebang (without verbose or debug)"
      ) || exit 999;
    }
    my ($output,$wstat) = capture_merged 
                    { system @rerun_cmd, "--internal", "--debug" };
    diag($output) if $wstat != 0;
    ok(($wstat==0 || diag(sprintf "** WAIT STATUS = 0x%04X\n", $wstat)
          and
        eval{ verif_no_internals_mentioned($output),1 } || diag($@))
       , "With --debug option"
    );
    exit 0;
  }
}

$SIG{__WARN__} = sub {
  die "bug:$_[0]" if $_[0] =~ "uninitialized value";
  print STDERR $_[0];
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
A title,Btitle,Multi Word Title C,,H,F,Gtitle,Z
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

require Exporter;
BEGIN { $Exporter::Debug = $debug }
use Text::CSV::Edit qw(:DEFAULT :STDVARS);
BEGIN { $Exporter::Debug = 0 }

BEGIN {
  #$Carp::Verbose = 1;
  $Carp::MaxArgLen = 0;
  $Carp::MaxArgNums = 25;

  if ($debug) {
    $Text::CSV::Edit::verbose = 1; # turn on for debugging implied new calls
    $Text::CSV::Edit::debug   = 1; # turn on for debugging implied new calls
  }
}

##########################################################################
package Other {
  BEGIN {*dprint = *main::dprint; *dprintf = *main::dprintf;}
  sub bug(@) { @_=("BUG ",@_); goto &Carp::confess }
  use Text::CSV::Edit qw(:DEFAULT); # don't import stdvars
  Text::CSV::Edit::tie_sheet_vars(['@rows','@myrows'], ['$num_cols', '$mync'],
                                  '%colx', '@linenums',
                                 );
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
  #"\nsheet=".Vis->vnew($s)->Maxdepth(2)->Dump
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

  # Access using the global %rowhash 
  my $v = $row{$key};
  my $vstr = vis( $v );

  # Access using the synonym %row
  { my $ovstr = vis($rowhash{$key});
    $err //= "\$row{$key} returned $vstr but $rowhash{key} returned $$ovstr"
      if $ovstr ne $vstr;
  }

  # Access using the overloaded hash-deref operator of the sheet object
  # (accesses the 'current' row)
  { my $ovstr = vis( sheet()->{$key} );
    $err //= "\$row{$key} returned $vstr but sheet()->{$key} returned $ovstr"
      if $vstr ne $ovstr;
  }

  # Access using the overloaded array-deref operator of the sheet object,
  # indexing the resulting Magicrow with the Title key.
  my $magicrow = sheet()->[$rx];
  { my $ovstr = vis( $magicrow->{$key} );
    $err //= "\$row{$key} returned $vstr but sheet()->[$rx]->{$key} returned $ovstr"
      if $vstr ne $ovstr;
  }

  # Index the Magicrow as an array indexed by cx
  { my $cx = $colx{$key};
    $err //= "%colx does not match sheet()->colx for key $key"
      if u($cx) ne u(sheet()->colx->{$key});
    my $ovstr = vis( defined($cx) ? $magicrow->[$cx] : undef );
    $err //= "\$row{$key} returned $vstr but sheet()->[$rx]->[$cx] returned $ovstr"
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
#   L implies data value, e.g. "C" implies values "C2","C3"..."C6"
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

  if ($L eq 'E') {
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
  confess dvis 'WRONG #COLUMNS @letters @$row $rx'
    if length($letters) != @$row;
  die "\$rx not right" unless ${ sheet() }->{current_rx} == $rx;

  for (my $cx=0, my $ABC="A"; $cx < length($letters); $cx++, $ABC++) {
    my $L = substr($letters,$cx,1);

    my ($ABC_v, $err) = getcell_byident($ABC);
    if ($@) { $err //= "ABC $ABC aborts ($@)" }
    elsif (! defined $ABC_v) { $err //= "ABC $ABC is undef" }

    # Manually locate the cell
    my $man_v = $row->[$cx];

    # The Titles    H, F, and Z mask the same-named ABC codes, and refer to
    # orig. columns E, F, and H .
    if ($L ne "*") { # data not irregular
      my $exp_v = "$L$rx"; # expected data value

      if ($man_v ne $exp_v) {
        $err //= svis 'WRONG DATA accessed by cx: Expecting $exp_v, got $man_v';
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
            $err //= svis('row{Title=$title} yields $vt but expecting $exp_v')
          }
        }
        if ($QT_usable) {
          die "bug" if $title_L eq "";
          my $qtitle = "'$title'";
          (my $vqt, $err) = getcell_bykey($qtitle, $err);
          if (u($vqt) ne $exp_v) {
            $err //= svis('row{QT=$qtitle} yields $vt expecting $exp_v')
          }
        }
      }
    }

    if (defined $err) {
      confess "BUG DETECTED...\n", fmtsheet(), "\n",
              #dvis('$rx $letters $cx $ABC $L $man_v $row->[$cx]\n$row\n'),
              $err;
    }
  }
  check_other_package();
}
sub check_titles($) {
  my $letters = shift;  # specifies current order of columns
  confess dvis('title_row is UNDEF\n').fmtsheet()
    unless defined $title_row;
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
      $err //= svis 'SHOULD HAVE EMPTY TITLE, not $title'
        unless $title eq "";
    } else {
      if ($title !~ /\Q$title_L\E/) {
        $err //= svis 'WRONG TITLE $title (expecting $title_L)'
      }
confess dvis 'BUG1' if ! defined $title;
confess dvis 'BUG2' if ! %colx;
confess "BUG3 title=",vis($title)," colx=",hvis(%colx) if ! defined($colx{$title});
warn 'OK: title=',vis($title)," colx=",hvis(%colx) if ! defined($colx{$title});
      if ($colx{$title} != $cx) {
        $err //= svis 'colx{$title} wrong (expecting $cx)'
      }
    }
    apply_torx {
      if ($row->[$cx] ne $title) {
        $err //= svis 'apply_torx title_rx : row->[$cx] is WRONG';
      }
    } $title_rx;
    apply_torx {
      if ($row->[$cx] ne $title) {
        $err //= svis 'apply_torx [title_rx] : row->[$cx] is WRONG';
      }
    } [$title_rx];
    if ($ABC_usable) {
      my $ABC = cx2let($cx);
      my $v = $colx{$ABC};
      $err //= svis('WRONG colx{ABC=$ABC} : Got $v, expecting $cx')
        unless u($v) eq $cx;
    }
    if ($QT_usable) {
      my $v = $colx{$qtitle};
      $err //= svis('WRONG colx{QT=$qtitle} : Got $v, expecting $cx')
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
      confess fmtsheet(), "\n", dvis('$L $cx $title_L \n   '), $err;
    }
  }
  check_other_package();
}

sub check_both($) {
  my $letters = shift;  # indicates current column ordering

  my $prev_verbose = options(verbose => 0);
  scope_guard { options(verbose => $prev_verbose) };

  croak "Expected $num_cols columns" unless length($letters) == $num_cols;

  check_titles $letters;

  apply {
    # FIXME shouldn't rx *always* be >= title_rx ???
    return if $rx < $title_rx;  # omit header rows
    check_currow_data($letters)
  };
}

sub verif_eval_err($) {
  my ($ln) = @_;
  my $fn = __FILE__;
  croak "expected error did not occur at line $ln\n" unless $@;

  if ($@ !~ / at $fn line $ln\.?(?:$|\n)/s) {
    croak "Got UN-expected err (did not point to file $fn line $ln): ",vis($@);
  } else {
    dprint "Got expected err: ",vis($@),"\n";
  }
  verif_no_internals_mentioned($@);
}

# Verify that a column title, alias, etc. is NOT defined
sub check_colspec_is_undef(@) {
  foreach(@_) {
    bug "Colspec ".vis($_)." is unexpectedly defined" 
      if defined $colx{$_};
    eval{ sheet()->colspectocxlist($_) };
    bug "colspectocxlist(".vis($_).") unexpectedly did not throw"
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
    outercroak "colx{$ident}=",vis($actual_cx),", expecting $cx (arg=",vis($_),")\n"
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
  use Text::CSV::Edit;
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug ;
  read_spreadsheet $main::infile;
  { (my $orig_ds = data_source) =~ /\Q$main::infile\E/ or die;
    data_source "New data source";
    data_source eq "New data source" or die;
    (data_source $orig_ds) eq $orig_ds or die;
    data_source eq $orig_ds or die;
  }
  title_rx("autodetect", last_cx => 2); # cx 3 has an empty title
  die "bug2" unless title_rx == 1;
};

# Test auto-detecting title row when alias() is called
package autodetect_test2 {
  use Text::CSV::Edit;
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug;
  read_spreadsheet $main::infile;
  autodetect_title_rx(last_cx => 2); # cx 3 has an empty title
  die "bug1" unless 1 == alias "_dummy" => "Btitle";
  die "bug2" unless title_rx == 1;
};

# Test auto-detecting title row on magic-row-hash deref
package autodetect_test3 {
  use Text::CSV::Edit;
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug;
  autodetect_title_rx(last_cx => 2); # cx 3 has an empty title
  read_spreadsheet $main::infile;
  die "bug1" unless sheet()->[2]->{Btitle} eq "B2";
  die "bug2" unless title_rx == 1;
};

# auto-detect title row on tied variable reference
package autodetect_test4 { 
  use Text::CSV::Edit qw(:DEFAULT :STDVARS);
  main::check_no_sheet;
  options silent => $main::silent, verbose => $main::debug;
  our $Btitle;
  tie_column_vars qw(Btitle); # sans $ sigl
  autodetect_title_rx(last_cx => 2); # cx 3 has an empty title
  read_spreadsheet $infile;
  apply_torx { die "bug1" unless $Btitle eq "B2" } 2;
  die "bug2" unless title_rx == 1;
}

# Check that tie_column_vars will refuse to tie pre-existing variables 
# when called in BEGIN{ ... }
package titleclash_test { 
  use Text::CSV::Edit qw(:DEFAULT :STDVARS);
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

# Test successful tie_column_vars without args in BEGIN{},
# implicitly importing all valid variables.
# We will use these (not explicitly declared) variables later.
BEGIN {
  check_no_sheet;
  options silent => $silent, verbose => $debug; # auto-create sheet
  read_spreadsheet $infile;
  
  # autodetect is not yet enabled, so title aliases are not valid
  eval { $_ = alias "_dummy" => "Btitle" }; verif_eval_err(__LINE__);

  { my $s=sheet(); dprint dvis('After reading $infile\n   $$s->{rows}\n   $$s->{colx_desc}\n'); }

  # Aliases are ok which do not reference titles (no title row yet)
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
    my $cxliststr = join " ", spectocxlist "A", "B", "C", "Aalias";
    die "spectocxlist (multi-match) wrong result: $cxliststr"
      unless $cxliststr eq "0 1 2 0";
  }

  # This must be in a BEGIN block so that titles will be auto-exported
  title_rx 1;

  # Test alias to title defined before being tied
  alias MWTCalia => qr/Multi Word .*C/;
  
  tie_column_vars;  # auto-tie whenever cols become defined

  { my $cxliststr = join " ", 
        spectocxlist qw(Aalias Aalia2 Dalias Ealias Falias),
                     qr/^[A-Z].*title/;
    die "spectocxlist (multi-match) wrong result: $cxliststr"
      unless $cxliststr eq "0 0 3 4 5 0 1 6";
  }
}# end BEGIN{}
apply_torx {
  die dvis '$MWTCalia is wrong' unless u($MWTCalia) eq "C2";
} 2;

# "H" is now a title for cx 4, so it maskes the ABC code "H".
# Pre-existing aliases remain pointing to their original columns.
alias Halia3 => 'H';

die "Halia3 gave wrong val"  unless sheet()->[2]->{Halia3} eq "E2";
die "Halias stopped working" unless sheet()->[2]->{Halias} eq "H2";
die "Halia2 stopped working" unless sheet()->[2]->{Halias} eq "H2";
die "Falias stopped working" unless sheet()->[2]->{Falias} eq "F2";

# "F" is also a now title for cx 5, but is the same as the ABC code
alias Falia3 => 'F';
die "Falia3 gave wrong val"  unless sheet()->[2]->{Falia3} eq "F2";
die "Falias stopped working" unless sheet()->[2]->{Falias} eq "F2";

# An alias created with a regexp matching titles, after tie_column_vars
my $Halia4cx = alias Halia4 => qr/^H$/;
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
    eval { my $i = $row }; verif_eval_err(__LINE__);
    eval { my $i = $linenum }; verif_eval_err(__LINE__);
    eval { my $i = $row->{A} }; verif_eval_err(__LINE__);
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
    new_rows 3,4;
    delete_rows 3,4,5,6;
    check_both('ABCDEFGH');

    new_rows 0,3;      # insert 3 rows at the top

    delete_rows 0..2;  # take them back out
    bug if $title_row->[5] ne 'F';
    check_both('ABCDEFGH');

# Append a new column
    our $Ktitle;  # will be tied
    new_cols '>$', "Ktitle";
    apply {
      $Ktitle = "K$rx";
    };
    check_both('ABCDEFGHK');

# Insert two new columns before that one
    our ($Ititle, $Jtitle); # will be tied
    new_cols 'Ktitle', qw(Ititle Jtitle);
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


    new_cols '^', "A title" ; apply { $A_title = "A$rx" };
    check_both('ABCEFHIJ');

    apply_all { return unless $rx==0; $row->[0] = "Restored initial stuff" };

    new_cols '>C',""; apply { $row->[3] = "D$rx" };
    check_both('ABCDEFHIJ');

    new_cols '>F', qw(Gtitle); apply { $Gtitle = "G$rx" };
    check_both('ABCDEFGHIJ');
    apply { bug unless $Gtitle eq "G$rx" };

    new_cols '>$', qw(Ktitle); apply { $Ktitle = "K$rx" };
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };

# only_cols

    only_cols qw(A B C D E F G Z I J K);   # (no-op)
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };

#sub ttt($) {
#  my $tag = shift;
#  warn "### $tag ", join(" ", map{vis} @{ $rows[title_rx] }),"\n";
#  #warn dvis '  %colx\n'; 
#  my %shown;
#  foreach(qw/A B C D E F G H I J K L Z/) {
#    my $cx = $colx{$_};
#    warn "  $_ →  ",u($colx_desc{$_}), 
#         (defined($cx) && !$shown{$cx}++
#            ? (",  rx2[$cx]=",vis($rows[2]->[$cx])) : ()), "\n"
#  };
#  foreach my $mycx (0..10) {
#    my @hits = grep{ $colx{$_} == $mycx } keys %colx;
#    warn "  cx $mycx <- ", join(" ", map{vis} @hits), "\n";
#  }
#}
    only_cols qw(K J I Z F E D C A B); # (re-arrange while deleting G)
    check_both('KJIHFEDCAB');
    apply { check_colspec_is_undef('Gtitle') };

    only_cols qw(8 9 7 6 5 4 3 2 1 0); # (un-re-arrange; G still omitted)
    check_both('ABCDEFHIJK');
    apply { check_colspec_is_undef('Gtitle') };

    # Restore col G
    new_cols '>F', "Gtitle" ; apply { $Gtitle = "G$rx" };
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

    rename_cols AAAtitle => "A title";
    check_both('ABCDEFGHIJK');
    apply { bug unless $Gtitle eq "G$rx" };
    check_other_package();

# switch sheet

    my $sheet1 = sheet();
    my $p = sheet();
    bug unless defined($p) && $p == $sheet1;
    bug unless $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__} == $sheet1;
    check_other_package();

    # replace with no sheet
    $p = sheet(undef);
    bug unless $p == $sheet1;
    bug if defined $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    bug if defined eval { my $x = $num_cols; } ; # expect undef or croak
    bug if defined eval { my $x = $A_title;   } ; # expect undef or croak
    bug if defined $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    $p = sheet();
    bug if defined $p;
    bug if defined $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__};
    bug if defined sheet();
    check_other_package();

    # put back the first sheet
    check_other_package();
    $p = sheet($sheet1);
    bug if defined $p;
    bug unless $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__} == $sheet1;
    apply { bug unless $Gtitle eq "G$rx" };
    check_both('ABCDEFGHIJK');

    # switch to a different sheet
    new_sheet { silent => $silent };
    my $sheet2 = $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__};
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
    bug unless $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__} == $sheet1;
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
    { my $obj = Text::CSV::Edit->new();
      bug if defined $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__};
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
      bug if defined $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__};
      bug if defined package_active_sheet(__PACKAGE__);
      bug if defined $Text::CSV::Edit::OO::pkg2currsheet{"".__PACKAGE__};
      bug if defined sheet();
    }

    # Create an empty sheet with defined num_cols, then add new rows
    my $old_num_cols = $sheet1->num_cols;
    my $old_rows = $sheet1->rows;
    my $old_num_rows = scalar @$old_rows;
    new_sheet(num_cols => $old_num_cols, silent => $silent);
      bug unless $num_cols == $old_num_cols;
      bug unless @rows == 0;
    new_rows 0, $old_num_rows;
      bug unless @rows == $old_num_rows;
    foreach (0..$old_num_rows-1) {
      $rows[$_] = $old_rows->[$_];
    }
    title_rx 1;
    check_both('ABCDEFGHIJK');

    # Create a sheet from existing data
    new_sheet(rows => $old_rows, silent => $silent);
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

# invert

    if ($debug) { print "Before invert:\n"; write_csv *STDOUT }
    invert;
    die if defined eval('$title_rx');
    if ($debug) { print "After  invert:\n"; write_csv *STDOUT }

    invert;
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
      $finaldata =~ s/"//g;
      $finaldata =~ s/^[^\n]*//;
      $testdata =~ s/^[^\n]*//;
      die dvis('\nWRONG DATA WRITTEN\n\n$finaldata\n\n $testdata\n').fmtsheet()
        unless $finaldata eq $testdata;
    }
    check_other_package();

# sort
{   package Other;
    dprint "> Running sort_rows test\n";

    sort_rows { 
                my ($p,$q)=@_; 
                Carp::confess("bug") unless defined($p);
                $myrows[$p]->[$colx{OtitleA}] <=> $myrows[$q]->[$colx{OtitleA}] 
              };
    #dprint dvis('### AFTER SORT:\n  @rows\n   @linenums\n');
    die "rows wrong after sort"
      unless main::arrays_eq [map{$_->[0]} @myrows], ["OtitleA",314,777,999];
    die "linenums wrong after sort"
      unless main::arrays_eq \@linenums, [1,3,4,2];
    my @Bs;
    apply { push @Bs, $OtitleB };
    die "apply broken after sort" unless main::arrays_eq \@Bs, [159, 888, 000];
}

exit 0;

