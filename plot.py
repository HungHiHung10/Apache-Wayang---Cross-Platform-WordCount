import csv
import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# =========================================================================
# 1. CONFIGURATION & SETUP
# =========================================================================
CSV_FILE_PATH = 'result/results.csv'
OUTPUT_IMAGE = 'result/benchmark_result.png'

PLATFORM_COLORS = {
    'JVM': '#FFC0CB',      # Light Pink
    'Flink': '#F4D03F',    # Yellow
    'Spark': '#BB8FCE',    # Purple
    'Wayang': '#7FB3D5'    # Light Blue
}

PLATFORMS = ['JVM', 'Flink', 'Spark', 'Wayang']

# =========================================================================
# 2. DATA LOADING
# =========================================================================
if not os.path.exists(CSV_FILE_PATH):
    print(f"[ERROR] Could not find {CSV_FILE_PATH}. Please run the Wayang benchmark first.")
    exit(1)

datasets = []
execution_times = {platform: [] for platform in PLATFORMS}
wayang_choices = []

with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        datasets.append(row['Dataset'])
        for platform in PLATFORMS:
            time_val = int(row[platform])
            execution_times[platform].append(max(time_val, 0))
        wayang_choices.append(row['Choice'])

# =========================================================================
# 3. PLOTTING SETUP
# =========================================================================
x_indices = np.arange(len(datasets))
bar_width = 0.2
offsets = [-1.5 * bar_width, -0.5 * bar_width, 0.5 * bar_width, 1.5 * bar_width]

# Calculate Y-axis limits dynamically based on data
all_times_flat = [time for times in execution_times.values() for time in times]
max_time = max(all_times_flat) if all_times_flat else 0
y_max_limit = int(np.ceil(max_time * 1.15 / 2000) * 2000)
y_ticks = np.arange(0, y_max_limit + 1, 2000)

fig, ax = plt.subplots(figsize=(11, 6.5), dpi=100)

# =========================================================================
# 4. DRAW BARS
# =========================================================================
bar_rects = []
for i, platform in enumerate(PLATFORMS):
    rects = ax.bar(x_indices + offsets[i], execution_times[platform], bar_width, 
                   label=platform, color=PLATFORM_COLORS[platform], edgecolor='none')
    bar_rects.append(rects)

# =========================================================================
# 5. DRAW WAYANG SELECTOR (BLOSSOM)
# =========================================================================
for idx, choice in enumerate(wayang_choices):
    if choice not in PLATFORMS:
        continue
    
    platform_idx = PLATFORMS.index(choice)
    val = execution_times[choice][idx]
    col_x = idx + offsets[platform_idx]
    
    # Distance to push the star up, based on Y-axis scale
    line_gap = y_max_limit * 0.05
    line_top = val + line_gap
    
    # Draw vertical line and blue star
    ax.vlines(col_x, val, line_top, colors='#0056B3', linestyles='-', linewidth=1.0)
    ax.plot(col_x, line_top, marker='*', color='#0056B3', markersize=9)

# =========================================================================
# 6. AUTOLABEL BAR HEIGHTS
# =========================================================================
for rects in bar_rects:
    for rect in rects:
        height = rect.get_height()
        if height == 0:
            continue
        ax.annotate(f'{int(height)}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8, fontweight='semibold', color='#333333')

# =========================================================================
# 7. CHART FORMATTING & EXPORT
# =========================================================================
# Add Professional Title
ax.set_title('Apache Wayang: Platform Adaptation Benchmark (WordCount)', fontsize=14, fontweight='bold', color='#1F3A60', pad=20)

ax.set_ylabel('Execution Time (ms)', fontsize=12, labelpad=10, fontweight='bold', color='#1F3A60')
ax.set_xlabel('Dataset Size', fontsize=12, labelpad=12, fontweight='bold', color='#1F3A60')

ax.set_xticks(x_indices)
ax.set_xticklabels(datasets, fontsize=11, fontweight='medium')
ax.set_yticks(y_ticks)
ax.tick_params(axis='y', labelsize=11)

# Subtle grid lines behind bars
ax.grid(axis='y', linestyle='-', linewidth=0.5, color='#EAEDED')
ax.set_axisbelow(True)

ax.set_ylim(0, y_max_limit)
ax.set_xlim(-0.6, len(datasets) - 0.4)

# Legend setup
blossom_legend = Line2D([0], [0], marker='*', color='w', markerfacecolor='#0056B3', markersize=11, label='Wayang Choice')
handles, labels_fig = ax.get_legend_handles_labels()
handles.insert(0, blossom_legend)

ax.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 1.15),
          ncol=5, frameon=False, fontsize=11, handletextpad=0.5, columnspacing=2.0)

# Remove top and right spines for a clean look
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_color('#BDC3C7')
ax.spines['bottom'].set_color('#BDC3C7')

plt.tight_layout()
plt.savefig(OUTPUT_IMAGE, dpi=150, bbox_inches='tight')
print(f"[INFO] Chart successfully saved to {OUTPUT_IMAGE}")