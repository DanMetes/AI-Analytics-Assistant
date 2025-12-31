"""Plot rendering utilities for the AI Analytics Assistant."""
import streamlit as st
from pathlib import Path
from collections import defaultdict


def get_plot_category(filename: str) -> str:
    """Categorize plot by its filename prefix."""
    name = filename.lower()
    if name.startswith("trend"):
        return "Trends"
    elif name.startswith("distribution"):
        return "Distributions"
    elif name.startswith("segmentation"):
        return "Segmentation"
    elif name.startswith("concentration"):
        return "Concentration"
    elif name.startswith("quality"):
        return "Quality Checks"
    else:
        return "Other"


def render_plots(run_path: Path) -> bool:
    """
    Render plots from a run with deduplication and selection UI.
    
    Args:
        run_path: Path to the run directory containing a plots/ subdirectory
        
    Returns:
        True if plots were found and rendered, False otherwise
    """
    plots_dir = run_path / "plots"
    
    if not plots_dir.exists():
        st.info("No plots directory found for this run.")
        return False
    
    all_plots = list(plots_dir.glob("*.png"))
    
    if not all_plots:
        st.info("No plots generated for this run.")
        return False
    
    unique_plots = {}
    for plot_path in all_plots:
        filename = plot_path.name
        if filename not in unique_plots:
            unique_plots[filename] = plot_path
    
    plots_by_category = defaultdict(list)
    for filename, plot_path in sorted(unique_plots.items()):
        category = get_plot_category(filename)
        plots_by_category[category].append((filename, plot_path))
    
    st.markdown(f"**{len(unique_plots)} plots available**")
    
    view_mode = st.radio(
        "View mode:",
        ["Select Plot", "Grid View"],
        horizontal=True,
        key="plot_view_mode"
    )
    
    if view_mode == "Select Plot":
        categories = list(plots_by_category.keys())
        
        if len(categories) > 1:
            selected_category = st.selectbox(
                "Category:",
                categories,
                key="plot_category"
            )
        else:
            selected_category = categories[0]
        
        category_plots = plots_by_category[selected_category]
        plot_options = [filename for filename, _ in category_plots]
        
        selected_plot = st.selectbox(
            "Plot:",
            plot_options,
            format_func=lambda x: x.replace("_", " ").replace(".png", "").title(),
            key="selected_plot"
        )
        
        if selected_plot:
            plot_path = unique_plots[selected_plot]
            st.image(
                str(plot_path), 
                caption=selected_plot.replace("_", " ").replace(".png", "").title(),
                use_container_width=True
            )
    
    else:
        for category, plots in sorted(plots_by_category.items()):
            st.markdown(f"**{category}**")
            
            num_cols = min(len(plots), 2)
            cols = st.columns(num_cols)
            
            for i, (filename, plot_path) in enumerate(plots):
                with cols[i % num_cols]:
                    st.image(
                        str(plot_path),
                        caption=filename.replace("_", " ").replace(".png", "").title(),
                        use_container_width=True
                    )
    
    return True
