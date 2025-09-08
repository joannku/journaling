"""
Simplified wellbeing change analysis using pre-filtered data.
Generates pre-post changes visualization without class structure.
"""

import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from scipy.stats import wilcoxon
import re
import warnings
warnings.filterwarnings('ignore')

# Setup paths

def _find_repo_root(start_path):
    """Walk upwards from start_path to find repo root (must contain 'data' and 'config')."""
    current = os.path.abspath(os.path.dirname(start_path))
    while True:
        if os.path.isdir(os.path.join(current, 'data')) and os.path.isdir(os.path.join(current, 'config')):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return os.path.abspath(os.path.dirname(start_path))
        current = parent

BASE_DIR = os.environ.get('JOURNALING_CORE_DIR', _find_repo_root(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
FIGURES_DIR = os.path.join(BASE_DIR, 'figures')

# Maximum points for each measure
MEASURE_MAX_POINTS = {
    'WEMWBS': 70,  # 14 items, 1-5 scale
    'GAD7': 21,    # 7 items, 0-3 scale
    'PHQ9': 27     # 9 items, 0-3 scale
}


def format_group_counts(df):
    """Return study group counts formatted as 'A: 42, B: 41, C: 37'."""
    try:
        counts = df['StudyGroup'].value_counts().to_dict()
        ordered_groups = ['A', 'B', 'C']
        parts = []
        for group in ordered_groups:
            if group in counts:
                parts.append(f"{group}: {int(counts[group])}")
        # Include any unexpected groups as well
        for group, count in counts.items():
            if group not in ordered_groups:
                parts.append(f"{group}: {int(count)}")
        return ", ".join(parts) if parts else "None"
    except Exception:
        return "None"


def load_filtered_data():
    """Load the pre-filtered participant data."""
    print("Loading pre-filtered participant data...")
    
    # Load the final filtered participants
    df_path = os.path.join(DATA_DIR, '13_participants_final_filtered.csv')
    if not os.path.exists(df_path):
        print(f"❌ Filtered data not found: {df_path}")
        print("Please run the preprocessing pipeline first: python src/preprocessing/final_filter.py")
        return None
    
    df = pd.read_csv(df_path)
    print(f"Loaded {len(df)} participants from pre-filtered dataset")
    print(f"Study groups: {format_group_counts(df)}")
    
    return df

def get_significance_label(p_value):
    """Get significance label for p-value."""
    if p_value < 0.001:
        return "***"
    elif p_value < 0.01:
        return "**"
    elif p_value < 0.05:
        return "*"
    else:
        return "ns"

def create_pre_post_changes_figure(df, display_fig=True, save_fig=True, show_participant_count=False, dpi=300):
    """Create main before/after boxplots matching original Plotly style."""
    print("Creating pre-post changes visualization...")
    
    # Define measure pairs
    pairs = [('B_WEMWBS_Total', 'E_WEMWBS_Total'), 
             ('B_GAD7_Total', 'E_GAD7_Total'), 
             ('B_PHQ9_Total', 'E_PHQ9_Total')]
    
    # Calculate p-values for significance annotations
    p_vals = {}
    for pair in pairs:
        stat, p_val = wilcoxon(df[pair[0]], df[pair[1]])
        pattern = r"_(.*)_"
        title = re.search(pattern, pair[1]).group(1)
        title_with_dash = re.sub(r'(\d+)', r'-\1', title)
        p_vals[title_with_dash] = p_val
    
    # Create subplot figure - we'll add custom titles with significance below
    fig = make_subplots(rows=1, cols=len(pairs))
    
    annotations = []
    
    # Loop through each pair and create boxplots
    for i, pair in enumerate(pairs, start=1):
        # Add boxplot for baseline
        fig.add_trace(go.Box(
            y=df[pair[0]],
            name='Pre-study',
            boxpoints='all',
            jitter=0.3,
            marker_color='blue',
            showlegend=False if i == 1 else False
        ), row=1, col=i)
        
        # Add boxplot for exit
        fig.add_trace(go.Box(
            y=df[pair[1]],
            name='Post-study',
            boxpoints='all',
            jitter=0.3,
            marker_color='red',
            showlegend=False if i == 1 else False
        ), row=1, col=i)
        
        # Calculate significance label
        pattern = r"_(.*)_"
        title = re.search(pattern, pair[1]).group(1)
        title_with_dash = re.sub(r'(\d+)', r'-\1', title)
        p_value = p_vals.get(title_with_dash)
        sig_label = get_significance_label(p_value)
        
        # Calculate direction of change and add directional arrow
        baseline_mean = df[pair[0]].mean()
        exit_mean = df[pair[1]].mean()
        
        # For WEMWBS (wellbeing), higher is better, so improvement = up arrow
        # For GAD7/PHQ9 (anxiety/depression), lower is better, so improvement = down arrow
        measure_code = re.search(pattern, pair[1]).group(1)
        if measure_code == 'WEMWBS':
            arrow = "↑" if exit_mean > baseline_mean else "↓"
        else:  # GAD7 or PHQ9
            arrow = "↓" if exit_mean < baseline_mean else "↑"
        
        # Get measure label for annotation
        measure_labels = {
            'WEMWBS': 'Wellbeing',
            'GAD7': 'Anxiety',
            'PHQ9': 'Depression'
        }
        measure_label = measure_labels.get(measure_code, measure_code)
        
        # Add condition label (title)
        annotations.append(dict(
            text=measure_label,
            x=0.5,
            y=1.08,
            xref=f'x{i}',
            yref='paper',
            showarrow=False,
            font=dict(size=16, color='black'),
            xanchor='center',
            yanchor='bottom'
        ))
        
        # Add significance annotation with directional arrow below the title
        if sig_label != "ns":
            sig_text = f"{arrow} {sig_label.strip()}"  # Remove the \n we added earlier
        else:
            sig_text = sig_label
            
        annotations.append(dict(
            text=sig_text,
            x=0.5,
            y=1.02,
            xref=f'x{i}',
            yref='paper',
            showarrow=False,
            font=dict(size=14, color='black' if sig_label != 'ns' else 'gray'),
            xanchor='center',
            yanchor='bottom'
        ))
        
        # Update axes: y-axis shows questionnaire name + Self-Report Score
        measure_code = re.search(pattern, pair[1]).group(1)
        measure_display = re.sub(r'(\d+)', r'-\1', measure_code)
        fig.update_yaxes(title_text=f"{measure_display}<br>Self-Report Score", row=1, col=i)
    
    # Apply annotations and layout
    fig.update_layout(annotations=annotations)
    
    # Final layout
    fig.update_layout(
        # title_text='Changes in Mental Health Scores: Pre-Post Study Comparison',
        template='plotly_white',
        height=400,
        width=1200
    )
    
    # Add participant count annotation (optional)
    if show_participant_count:
        fig.add_annotation(
            text=f'Total Participants: {len(df)}',
            xref='paper',
            yref='paper',
            x=0.95,
            y=-0.35,
            showarrow=False,
            font=dict(size=12)
        )
    
    # Display figure
    if display_fig:
        fig.show()
    
    # Save figure
    if save_fig:
        os.makedirs(FIGURES_DIR, exist_ok=True)
        output_path = os.path.join(FIGURES_DIR, 'wellbeing_pre_post_changes.png')
        # Calculate scale factor to achieve desired DPI (default plotly DPI is ~72)
        scale_factor = dpi / 72
        pio.write_image(fig, output_path, width=1200, height=400, scale=scale_factor)
        print(f"✅ Pre-post changes figure saved to: {output_path} (DPI: {dpi})")
    
    return fig

def create_before_after_scatterplots(df, display_fig=True, save_fig=True, show_participant_count=False, dpi=400):
    """Create before/after scatterplots by study group with y=x reference line."""
    print("Creating before/after scatterplots...")

    pairs = [
        ('B_WEMWBS_Total', 'E_WEMWBS_Total', 'WEMWBS'),
        ('B_GAD7_Total', 'E_GAD7_Total', 'GAD7'),
        ('B_PHQ9_Total', 'E_PHQ9_Total', 'PHQ9')
    ]

    # Display names and axis ranges per measure
    display_names = {
        'WEMWBS': 'Wellbeing',
        'GAD7': 'Anxiety',
        'PHQ9': 'Depression'
    }
    axis_ranges = {
        'WEMWBS': (0, 72, 10),   # (min, max, tick)
        'GAD7': (0, 22, 3),
        'PHQ9': (0, 28, 3)
    }
    # Questionnaire labels for axis titles
    questionnaire_labels = {
        'WEMWBS': 'WEMWBS',
        'GAD7': 'GAD-7',
        'PHQ9': 'PHQ-9'
    }

    # Color mapping for groups
    colors = {
        'A': 'orchid',
        'B': 'turquoise',
        'C': 'orange'
    }

    # Legend labels
    coding = {
        'A': 'Cognitive Summary',
        'B': 'Emotional Summary',
        'C': 'No Summary'
    }

    fig = make_subplots(rows=1, cols=3, subplot_titles=[display_names[p[2]] for p in pairs])

    sorted_groups = sorted(df['StudyGroup'].dropna().unique())

    for i, (base_col, exit_col, code) in enumerate(pairs, start=1):
        # Scatter points per group
        for j, group in enumerate(sorted_groups):
            group_data = df[df['StudyGroup'] == group]
            fig.add_trace(
                go.Scatter(
                    x=group_data[base_col],
                    y=group_data[exit_col],
                    mode='markers',
                    name=coding.get(group, str(group)),
                    marker=dict(color=colors.get(group, 'gray'), size=8, symbol='circle', opacity=0.85),
                    showlegend=True if i == 1 else False
                ),
                row=1, col=i
            )

        # y = x reference line using axis range
        xmin, xmax, _ = axis_ranges[code]
        fig.add_trace(
            go.Scatter(
                x=[xmin, xmax],
                y=[xmin, xmax],
                mode='lines',
                line=dict(dash='dash', color='black'),
                name='No Change',
                showlegend=True if i == 1 else False
            ),
            row=1, col=i
        )

        # Axis labels and identical ranges/ticks
        fig.update_xaxes(
            title_text=f'Pre-study {questionnaire_labels[code]} Scores',
            range=[xmin, xmax],
            tick0=xmin,
            dtick=axis_ranges[code][2],
            row=1, col=i
        )
        fig.update_yaxes(
            title_text=f'Post-study {questionnaire_labels[code]} Scores',
            range=[xmin, xmax],
            tick0=xmin,
            dtick=axis_ranges[code][2],
            row=1, col=i
        )

    fig.update_layout(
        # title_text='Before-and-After Comparison of Scores',
        template='plotly_white',
        height=420,
        width=1300,
        legend=dict(
            orientation='h',
            yanchor='top',
            y=-0.2,
            xanchor='center',
            x=0.5,
            bordercolor='rgba(0,0,0,0)'
        ),
        # margin=dict(l=60, r=20, t=60, b=110)
    )

    # Add participant count annotation (optional, match position with pre-post figure)
    if show_participant_count:
        fig.add_annotation(
            text=f'Total Participants: {len(df)}',
            xref='paper', yref='paper',
            x=0.95, y=-0.35,
            showarrow=False,
            font=dict(size=12)
        )

    if display_fig:
        fig.show()

    if save_fig:
        os.makedirs(FIGURES_DIR, exist_ok=True)
        out_path = os.path.join(FIGURES_DIR, 'wellbeing_before_after_scatterplots.png')
        # Calculate scale factor to achieve desired DPI (default plotly DPI is ~72)
        scale_factor = dpi / 72
        pio.write_image(fig, out_path, width=1300, height=420, scale=scale_factor)
        print(f"Before/after scatterplots saved to {out_path} (DPI: {dpi})")

    return fig

def print_summary_statistics(df):
    """Print summary statistics for the changes."""
    print("\n" + "="*60)
    print("PRE-POST WELLBEING CHANGES SUMMARY")
    print("="*60)
    
    measures = ['WEMWBS', 'GAD7', 'PHQ9']
    
    print(f"\nDataset: {len(df)} participants")
    print(f"Study groups: {format_group_counts(df)}")
    
    print(f"\n{'Measure':<10} {'Max':<5} {'Pre-Mean':<10} {'Pre-SD':<10} {'Post-Mean':<10} {'Post-SD':<10} {'Change':<10} {'%Change(Max)':<13} {'P-Value':<10} {'Significance':<12}")
    print("-" * 123)
    
    for measure in measures:
        baseline_col = f'B_{measure}_Total'
        exit_col = f'E_{measure}_Total'
        
        baseline_mean = df[baseline_col].mean()
        exit_mean = df[exit_col].mean()
        baseline_sd = df[baseline_col].std()
        exit_sd = df[exit_col].std()
        change = exit_mean - baseline_mean
        max_points = MEASURE_MAX_POINTS.get(measure, np.nan)
        pct_change = (change / max_points * 100.0) if max_points and not np.isnan(max_points) else np.nan
        
        # Perform Wilcoxon test
        stat, p_val = wilcoxon(df[baseline_col], df[exit_col])
        sig_label = get_significance_label(p_val)
        
        print(f"{measure:<10} {max_points!s:<5} {baseline_mean:<10.2f} {baseline_sd:<10.2f} {exit_mean:<10.2f} {exit_sd:<10.2f} {change:<+10.2f} {pct_change:<13.2f} {p_val:<10.4f} {sig_label:<12}")
    
    print("\nInterpretation:")
    print("• WEMWBS: Higher scores = better wellbeing")
    print("• GAD7: Lower scores = less anxiety") 
    print("• PHQ9: Lower scores = less depression")
    print("• Significance: *** p<0.001, ** p<0.01, * p<0.05, ns = not significant")

def main(show_participant_count=False, dpi=400):
    """Main function to run the simplified wellbeing analysis.
    
    Args:
        show_participant_count (bool): Whether to display total participant count on plots. Default: False.
        dpi (int): DPI resolution for saved figures. Default: 300.
    """
    print("Starting simplified wellbeing pre-post analysis...")
    
    # Load pre-filtered data
    df = load_filtered_data()
    if df is None:
        return
    
    # Create the main visualization
    fig = create_pre_post_changes_figure(df, display_fig=True, save_fig=True, show_participant_count=show_participant_count, dpi=dpi)

    # Create before/after scatterplots figure (as in stats.ipynb)
    create_before_after_scatterplots(df, display_fig=True, save_fig=True, show_participant_count=show_participant_count, dpi=dpi)

    # Print summary statistics
    print_summary_statistics(df)
    
    print(f"\n✅ Analysis complete!")
    print(f"   • Generated figure: wellbeing_pre_post_changes.png")

if __name__ == "__main__":
    main()
