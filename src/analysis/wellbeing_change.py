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

def create_pre_post_changes_figure(df, display_fig=True, save_fig=True):
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
    
    # Add participant count annotation
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
        pio.write_image(fig, output_path, scale=5)
        print(f"✅ Pre-post changes figure saved to: {output_path}")
    
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

def main():
    """Main function to run the simplified wellbeing analysis."""
    print("Starting simplified wellbeing pre-post analysis...")
    
    # Load pre-filtered data
    df = load_filtered_data()
    if df is None:
        return
    
    # Create the main visualization
    fig = create_pre_post_changes_figure(df, display_fig=True, save_fig=True)
    
    # Print summary statistics
    print_summary_statistics(df)
    
    print(f"\n✅ Analysis complete!")
    print(f"   • Generated figure: wellbeing_pre_post_changes.png")

if __name__ == "__main__":
    main()
