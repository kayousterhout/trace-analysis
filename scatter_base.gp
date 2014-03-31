set terminal pdfcairo font 'Times,22' linewidth 4 rounded dashed dashlength 2

# Line style for axes
set style line 80 lt 1 lc rgb "#808080"

# Line style for grid
set style line 81 lt 0 # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
    # Also, put it in grey; no need for so much emphasis on a border.
set xtics nomirror
set ytics nomirror

set style line 1 lt rgb "#E41A1C" lw 2 pt 1
set style line 2 lt rgb "#377EB8" lw 2 pt 6
set style line 3 lt rgb "#4DAF4A" lw 2 pt 2
set style line 4 lt rgb "#984EA3" lw 2 pt 9

set xlabel "Data (KB)" offset 0,0.5
set ylabel "Time (ms)" offset 0.5,0
set key above

