set terminal pdfcairo font 'Times,22' linewidth 4 rounded dashlength 2 size 10,20

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

set style line 1 lt rgb "#fc8d62" lw 1 pt 1
set style line 2 lt rgb "#8da0cb" lw 1 pt 6
set style line 3 lt rgb "#e78ac3" lw 1 pt 2
set style line 4 lt rgb "#a6d854" lw 1 pt 3
set style line 5 lt rgb "#66c2a5" lw 1 pt 4

set xlabel "Time (ms)" offset 0,0.5
set key above
set output "tasks_time.pdf"

set arrow from 1,1 to 3,1 ls 1 nohead
set arrow from 3,1 to 6,1 ls 2 nohead
set arrow from 6,1 to 8,1 ls 3 nohead
set arrow from 8,1 to 11,1 ls 4 nohead
set arrow from 11,1 to 13,1 ls 5 nohead

