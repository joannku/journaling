"""
Wellbeing analysis by study group.
Generates descriptive statistics and Wilcoxon test results for each study group.
"""

import os
import pandas as pd
import numpy as np
from scipy.stats import wilcoxon
from statsmodels.stats.multitest import multipletests
from scipy import stats
import pingouin as pg
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

def cohens_d(before, after):
    """Calculate Cohen's d effect size for paired samples using pingouin."""
    # pingouin's compute_effsize with paired=True uses the appropriate formula for repeated measures
    return pg.compute_effsize(before, after, paired=True, eftype='cohen')

def paired_confidence_interval(before, after, confidence=0.95):
    """Calculate confidence interval for paired mean difference."""
    diff = after - before
    mean_diff = diff.mean()
    se_diff = stats.sem(diff)
    alpha = 1 - confidence
    t_critical = stats.t.ppf(1 - alpha/2, len(diff) - 1)
    margin_error = t_critical * se_diff
    ci_lower = mean_diff - margin_error
    ci_upper = mean_diff + margin_error
    return ci_lower, ci_upper

def calculate_group_statistics(df):
    """
    Calculate descriptive statistics and Wilcoxon test results for each study group.
    Returns a formatted table with FDR BH correction applied.
    """
    
    # Define measures and their baseline/exit column pairs
    measures = {
        'WEMWBS (14-70)': ('B_WEMWBS_Total', 'E_WEMWBS_Total'),
        'GAD-7 (0-21)': ('B_GAD7_Total', 'E_GAD7_Total'), 
        'PHQ-9 (0-27)': ('B_PHQ9_Total', 'E_PHQ9_Total')
    }
    
    # Group labels mapping
    group_labels = {
        'A': 'Cognitive Sum.',
        'B': 'Emotional Sum.',
        'C': 'No Sum.'
    }
    
    results = []
    p_values_for_correction = []
    
    # First pass: Calculate all statistics and collect p-values
    for measure_name, (baseline_col, exit_col) in measures.items():
        # All groups
        baseline_mean = df[baseline_col].mean()
        baseline_std = df[baseline_col].std()
        exit_mean = df[exit_col].mean()
        exit_std = df[exit_col].std()
        mean_diff = exit_mean - baseline_mean
        
        # Calculate percentage change (need to determine appropriate max score)
        if 'WEMWBS' in measure_name:
            max_score = 70
        elif 'GAD-7' in measure_name:
            max_score = 21
        elif 'PHQ-9' in measure_name:
            max_score = 27
            
        pct_change = (mean_diff / max_score) * 100
        
        # Wilcoxon test
        stat, p_value = wilcoxon(df[baseline_col], df[exit_col])
        p_values_for_correction.append(p_value)
        
        # Effect size and confidence interval
        effect_size = cohens_d(df[baseline_col], df[exit_col])
        ci_lower, ci_upper = paired_confidence_interval(df[baseline_col], df[exit_col])
        
        results.append({
            'Measure': measure_name,
            'Study Group': 'All groups',
            'No. Ppt.': len(df),
            'Baseline Mean': f"{baseline_mean:.2f}",
            'Baseline St.Dev': f"{baseline_std:.2f}",
            'Exit Mean': f"{exit_mean:.2f}",
            'Exit St.Dev': f"{exit_std:.2f}",
            'Mean Diff.': f"{mean_diff:+.2f}",
            '%': f"{pct_change:+.1f}",
            'Cohen\'s d': f"{effect_size:.3f}",
            '95% CI': f"[{ci_lower:.2f}, {ci_upper:.2f}]",
            'Z-Stat.': f"{stat:.1f}",
            'P-Value': f"{p_value:.4f}",
            'p_value_raw': p_value  # Store raw p-value for correction
        })
        
        # Calculate for each study group
        for group in ['A', 'B', 'C']:
            group_data = df[df['StudyGroup'] == group]
            if len(group_data) == 0:
                continue
                
            baseline_mean = group_data[baseline_col].mean()
            baseline_std = group_data[baseline_col].std()
            exit_mean = group_data[exit_col].mean()
            exit_std = group_data[exit_col].std()
            mean_diff = exit_mean - baseline_mean
            pct_change = (mean_diff / max_score) * 100
            
            # Wilcoxon test for this group
            if len(group_data) > 5:  # Need sufficient sample size
                stat, p_value = wilcoxon(group_data[baseline_col], group_data[exit_col])
                p_values_for_correction.append(p_value)
                
                # Effect size and confidence interval for this group
                effect_size = cohens_d(group_data[baseline_col], group_data[exit_col])
                ci_lower, ci_upper = paired_confidence_interval(group_data[baseline_col], group_data[exit_col])
                
                effect_size_str = f"{effect_size:.3f}"
                ci_str = f"[{ci_lower:.2f}, {ci_upper:.2f}]"
                z_stat_str = f"{stat:.1f}"
                p_value_str = f"{p_value:.4f}"
            else:
                stat, p_value = np.nan, np.nan
                p_values_for_correction.append(np.nan)
                effect_size_str = "N/A"
                ci_str = "N/A"
                z_stat_str = "N/A"
                p_value_str = "N/A"
            
            results.append({
                'Measure': '',  # Empty for grouped rows
                'Study Group': group_labels[group],
                'No. Ppt.': len(group_data),
                'Baseline Mean': f"{baseline_mean:.2f}",
                'Baseline St.Dev': f"{baseline_std:.2f}",
                'Exit Mean': f"{exit_mean:.2f}",
                'Exit St.Dev': f"{exit_std:.2f}",
                'Mean Diff.': f"{mean_diff:+.2f}",
                '%': f"{pct_change:+.1f}",
                'Cohen\'s d': effect_size_str,
                '95% CI': ci_str,
                'Z-Stat.': z_stat_str,
                'P-Value': p_value_str,
                'p_value_raw': p_value if not np.isnan(p_value) else None
            })
    
    # Apply FDR BH correction
    valid_p_values = [p for p in p_values_for_correction if not np.isnan(p)]
    if valid_p_values:
        rejected, corrected_p_values, alpha_sidak, alpha_bonf = multipletests(
            valid_p_values, alpha=0.05, method='fdr_bh'
        )
        
        # Create mappings from original p-values to corrected values and significance
        p_value_to_corrected = {}
        p_value_to_significance = {}
        for i, p_val in enumerate(valid_p_values):
            p_value_to_corrected[p_val] = corrected_p_values[i]
            p_value_to_significance[p_val] = rejected[i]
    
    # Add corrected p-values and significance to results
    for i, result in enumerate(results):
        raw_p = result.get('p_value_raw')
        if raw_p is not None and not np.isnan(raw_p):
            corrected_p = p_value_to_corrected.get(raw_p, raw_p)
            is_significant = p_value_to_significance.get(raw_p, False)
            
            result['P-Value (FDR)'] = f"{corrected_p:.4f}"
            result['Sig.'] = get_significance_label(corrected_p)
        else:
            result['P-Value (FDR)'] = "N/A"
            result['Sig.'] = "N/A"
        
        # Remove the temporary raw p-value
        del result['p_value_raw']
    
    return pd.DataFrame(results)

def print_statistics_table(df, save_csv=True):
    """Print the statistics table and optionally save as CSV."""
    stats_df = calculate_group_statistics(df)
    
    print("\nTable 5. Descriptive Statistics and results of individual Wilcoxon tests displaying the effect of group")
    print("assignment (Cognitive Summary, Emotional Summary, and No Summary) on mental health outcomes.")
    print("Baseline and exit metrics for overall engagement, WEMWBS, GAD-7, and PHQ-9 are provided, including")
    print("participant count, mean values, standard deviations, effect sizes (Cohen's d), confidence intervals,")
    print("Z-statistics, raw p-values, FDR-corrected p-values, and significance levels.")
    print("\n" + "="*155)
    
    # Print header
    header = f"{'Measure':<15} {'Study Group':<12} {'No. Ppt.':<8} {'Baseline Mean':<13} {'Baseline St.Dev':<15} {'Exit Mean':<10} {'Exit St.Dev':<10} {'Mean Diff.':<10} {'%':<6} {'Cohen\'s d':<10} {'95% CI':<18} {'Z-Stat.':<8} {'P-Value':<8} {'P-Value (FDR)':<13} {'Sig.':<5}"
    print(header)
    print("-" * 155)
    
    # Print data rows
    for _, row in stats_df.iterrows():
        print(f"{row['Measure']:<15} {row['Study Group']:<12} {row['No. Ppt.']:<8} {row['Baseline Mean']:<13} {row['Baseline St.Dev']:<15} {row['Exit Mean']:<10} {row['Exit St.Dev']:<10} {row['Mean Diff.']:<10} {row['%']:<6} {row['Cohen\'s d']:<10} {row['95% CI']:<18} {row['Z-Stat.']:<8} {row['P-Value']:<8} {row['P-Value (FDR)']:<13} {row['Sig.']:<5}")
    
    # Save as CSV
    if save_csv:
        os.makedirs(FIGURES_DIR, exist_ok=True)
        csv_path = os.path.join(FIGURES_DIR, 'wellbeing_group_statistics.csv')
        stats_df.to_csv(csv_path, index=False)
        print(f"\n✅ Statistics table saved to: {csv_path}")
    
    return stats_df

def main():
    """Main function to run the wellbeing by group analysis."""
    print("Starting wellbeing analysis by study group...")
    
    # Load pre-filtered data
    df = load_filtered_data()
    if df is None:
        return
    
    # Generate and print statistics table
    print_statistics_table(df)
    
    print(f"\n✅ Group analysis complete!")

if __name__ == "__main__":
    main()
