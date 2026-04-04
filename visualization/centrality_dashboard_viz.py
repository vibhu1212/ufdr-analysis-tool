"""
Advanced Centrality Dashboard Module
Comprehensive visualization of network centrality metrics to identify key players
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import sqlite3
import os
import sys
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class CentralityDashboard:
    """Creates comprehensive centrality analysis dashboards"""
    
    def __init__(self, db_path='forensic_data.db'):
        self.db_path = db_path
        self.output_dir = 'visualization/output'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def create_centrality_overview(self, case_id, top_n=20):
        """
        Create comprehensive centrality metrics dashboard
        
        Args:
            case_id: Case identifier
            top_n: Number of top contacts to show
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n📊 Creating centrality dashboard for {case_id}...")
        
        # Import graph analyzer
        from visualization.graph_analytics import ForensicGraphAnalyzer
        
        # Build graph
        analyzer = ForensicGraphAnalyzer(db_path=self.db_path)
        G = analyzer.build_communication_graph(case_id, min_interactions=1)
        
        if G.number_of_nodes() == 0:
            print("   ⚠️  No network data found")
            return None
        
        # Calculate all centrality metrics
        metrics = analyzer.calculate_centrality_metrics(G, top_n=top_n)
        
        # Get contact names
        conn = sqlite3.connect(self.db_path)
        
        # Prepare data for visualization
        centrality_data = defaultdict(lambda: {
            'degree': 0,
            'betweenness': 0,
            'closeness': 0,
            'pagerank': 0,
            'eigenvector': 0,
            'name': ''
        })
        
        # Collect all unique contacts
        all_contacts = set()
        for metric_name, values in metrics.items():
            for contact, score in values:
                all_contacts.add(contact)
        
        # Fill in data
        for metric_name, values in metrics.items():
            metric_key = metric_name.replace('_centrality', '').replace('pagerank', 'pagerank')
            for contact, score in values:
                centrality_data[contact][metric_key] = score
        
        # Get names for all contacts in batches
        contact_list = list(all_contacts)
        batch_size = 900

        # Pre-fill with default values
        for contact in contact_list:
            centrality_data[contact]['name'] = contact[:15]

        for i in range(0, len(contact_list), batch_size):
            batch = contact_list[i:i+batch_size]
            placeholders = ','.join(['?'] * len(batch))
            cursor = conn.execute(
                f"SELECT phone_digits, COALESCE(name, phone_raw) FROM contacts WHERE phone_digits IN ({placeholders})",
                batch
            )
            for row in cursor.fetchall():
                phone_digits, name = row
                if phone_digits in centrality_data:
                    centrality_data[phone_digits]['name'] = name
        
        conn.close()
        
        # Create DataFrame
        df = pd.DataFrame([
            {
                'contact': contact,
                'name': data['name'],
                'degree': data['degree'],
                'betweenness': data['betweenness'],
                'closeness': data['closeness'],
                'pagerank': data['pagerank'],
                'eigenvector': data['eigenvector']
            }
            for contact, data in centrality_data.items()
        ])
        
        # Calculate composite score (average of normalized metrics)
        df['composite_score'] = df[['degree', 'betweenness', 'closeness', 'pagerank', 'eigenvector']].mean(axis=1)
        df = df.sort_values('composite_score', ascending=False).head(top_n)
        
        # Create comprehensive dashboard
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                'Composite Importance Score', 
                'Degree Centrality (Connections)',
                'Betweenness Centrality (Bridge Role)', 
                'PageRank (Influence)',
                'Centrality Metrics Comparison',
                'Top Key Players Summary'
            ),
            specs=[
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "table"}]
            ],
            vertical_spacing=0.12,
            horizontal_spacing=0.15
        )
        
        # 1. Composite Score - Top 15
        top_composite = df.nlargest(15, 'composite_score')
        fig.add_trace(
            go.Bar(
                x=top_composite['name'],
                y=top_composite['composite_score'],
                marker_color='purple',
                text=[f"{score:.3f}" for score in top_composite['composite_score']],
                textposition='outside',
                showlegend=False
            ),
            row=1, col=1
        )
        
        # 2. Degree Centrality
        top_degree = df.nlargest(15, 'degree')
        fig.add_trace(
            go.Bar(
                x=top_degree['name'],
                y=top_degree['degree'],
                marker_color='blue',
                text=[f"{score:.3f}" for score in top_degree['degree']],
                textposition='outside',
                showlegend=False
            ),
            row=1, col=2
        )
        
        # 3. Betweenness Centrality
        top_between = df.nlargest(15, 'betweenness')
        fig.add_trace(
            go.Bar(
                x=top_between['name'],
                y=top_between['betweenness'],
                marker_color='red',
                text=[f"{score:.3f}" for score in top_between['betweenness']],
                textposition='outside',
                showlegend=False
            ),
            row=2, col=1
        )
        
        # 4. PageRank
        top_pagerank = df.nlargest(15, 'pagerank')
        fig.add_trace(
            go.Bar(
                x=top_pagerank['name'],
                y=top_pagerank['pagerank'],
                marker_color='green',
                text=[f"{score:.3f}" for score in top_pagerank['pagerank']],
                textposition='outside',
                showlegend=False
            ),
            row=2, col=2
        )
        
        # 5. Scatter plot comparing two key metrics
        fig.add_trace(
            go.Scatter(
                x=df['degree'],
                y=df['betweenness'],
                mode='markers+text',
                marker=dict(
                    size=df['pagerank'] * 1000,  # Size by PageRank
                    color=df['composite_score'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Composite<br>Score", x=0.46)
                ),
                text=df['name'],
                textposition='top center',
                textfont=dict(size=8),
                showlegend=False
            ),
            row=3, col=1
        )
        
        # 6. Summary table - Top 10
        top_10 = df.nlargest(10, 'composite_score')
        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Rank', 'Name', 'Composite', 'Degree', 'Between.', 'PageRank'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=10, color='black')
                ),
                cells=dict(
                    values=[
                        list(range(1, len(top_10) + 1)),
                        top_10['name'].tolist(),
                        [f"{x:.3f}" for x in top_10['composite_score']],
                        [f"{x:.3f}" for x in top_10['degree']],
                        [f"{x:.3f}" for x in top_10['betweenness']],
                        [f"{x:.3f}" for x in top_10['pagerank']]
                    ],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=9)
                )
            ),
            row=3, col=2
        )
        
        # Update layout
        fig.update_layout(
            title=f"Advanced Centrality Dashboard - {case_id}",
            height=1200,
            showlegend=False
        )
        
        # Update axes
        fig.update_xaxes(tickangle=-45, row=1, col=1)
        fig.update_xaxes(tickangle=-45, row=1, col=2)
        fig.update_xaxes(tickangle=-45, row=2, col=1)
        fig.update_xaxes(tickangle=-45, row=2, col=2)
        
        fig.update_yaxes(title_text="Score", row=1, col=1)
        fig.update_yaxes(title_text="Score", row=1, col=2)
        fig.update_yaxes(title_text="Score", row=2, col=1)
        fig.update_yaxes(title_text="Score", row=2, col=2)
        fig.update_xaxes(title_text="Degree Centrality", row=3, col=1)
        fig.update_yaxes(title_text="Betweenness Centrality", row=3, col=1)
        
        # Save
        output_path = os.path.join(self.output_dir, f'centrality_dashboard_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Centrality dashboard complete: {output_path}")
        print(f"   📊 Analyzed {len(df)} key contacts")
        
        return output_path
    
    def create_individual_profile(self, case_id, contact_digits, top_n=20):
        """
        Create detailed centrality profile for a specific contact
        
        Args:
            case_id: Case identifier
            contact_digits: Phone digits to analyze
            top_n: Number of contacts for comparison
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n👤 Creating centrality profile for {contact_digits}...")
        
        # Import graph analyzer
        from graph_analytics import ForensicGraphAnalyzer
        
        # Build graph
        analyzer = ForensicGraphAnalyzer(db_path=self.db_path)
        G = analyzer.build_communication_graph(case_id, min_interactions=1)
        
        if contact_digits not in G.nodes():
            print(f"   ⚠️  Contact {contact_digits} not found in network")
            return None
        
        # Calculate all centrality metrics
        metrics = analyzer.calculate_centrality_metrics(G, top_n=100)  # Get more for ranking
        
        # Get contact info
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT COALESCE(name, phone_raw), phone_raw FROM contacts WHERE phone_digits = ? LIMIT 1",
            (contact_digits,)
        )
        result = cursor.fetchone()
        contact_name = result[0] if result else contact_digits
        contact_phone = result[1] if result else contact_digits
        conn.close()
        
        # Extract scores for this contact
        contact_scores = {
            'degree': 0,
            'betweenness': 0,
            'closeness': 0,
            'pagerank': 0,
            'eigenvector': 0
        }
        
        contact_ranks = {
            'degree': 0,
            'betweenness': 0,
            'closeness': 0,
            'pagerank': 0,
            'eigenvector': 0
        }
        
        for metric_name, values in metrics.items():
            metric_key = metric_name.replace('_centrality', '').replace('pagerank', 'pagerank')
            for rank, (contact, score) in enumerate(values, 1):
                if contact == contact_digits:
                    contact_scores[metric_key] = score
                    contact_ranks[metric_key] = rank
                    break
        
        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                f'Centrality Scores for {contact_name}',
                'Rankings Across Metrics',
                'Radar Chart - Importance Profile',
                'Contact Information'
            ),
            specs=[
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "scatterpolar"}, {"type": "table"}]
            ]
        )
        
        # 1. Scores bar chart
        metrics_list = list(contact_scores.keys())
        scores_list = list(contact_scores.values())
        
        colors = ['blue', 'red', 'green', 'purple', 'orange']
        
        fig.add_trace(
            go.Bar(
                x=metrics_list,
                y=scores_list,
                marker_color=colors,
                text=[f"{s:.4f}" for s in scores_list],
                textposition='outside',
                showlegend=False
            ),
            row=1, col=1
        )
        
        # 2. Rankings bar chart (lower is better)
        ranks_list = list(contact_ranks.values())
        
        fig.add_trace(
            go.Bar(
                x=metrics_list,
                y=ranks_list,
                marker_color=colors,
                text=[f"#{r}" for r in ranks_list],
                textposition='outside',
                showlegend=False
            ),
            row=1, col=2
        )
        
        # 3. Radar chart
        # Normalize scores to 0-1 for radar chart
        max_scores = {k: max(v) for k, v in {
            'degree': [s for _, s in metrics.get('degree_centrality', [(None, 0)])],
            'betweenness': [s for _, s in metrics.get('betweenness_centrality', [(None, 0)])],
            'closeness': [s for _, s in metrics.get('closeness_centrality', [(None, 0)])],
            'pagerank': [s for _, s in metrics.get('pagerank', [(None, 0)])],
            'eigenvector': [s for _, s in metrics.get('eigenvector_centrality', [(None, 0)])]
        }.items()}
        
        normalized_scores = [
            contact_scores[k] / max_scores[k] if max_scores[k] > 0 else 0
            for k in metrics_list
        ]
        
        fig.add_trace(
            go.Scatterpolar(
                r=normalized_scores + [normalized_scores[0]],  # Close the loop
                theta=metrics_list + [metrics_list[0]],
                fill='toself',
                marker_color='rgba(0, 100, 255, 0.6)',
                line=dict(color='blue', width=2),
                showlegend=False
            ),
            row=2, col=1
        )
        
        # 4. Contact information table
        info_data = [
            ['Name', contact_name],
            ['Phone', contact_phone],
            ['', ''],
            ['Degree Rank', f"#{contact_ranks['degree']}"],
            ['Betweenness Rank', f"#{contact_ranks['betweenness']}"],
            ['Closeness Rank', f"#{contact_ranks['closeness']}"],
            ['PageRank Rank', f"#{contact_ranks['pagerank']}"],
            ['Eigenvector Rank', f"#{contact_ranks['eigenvector']}"]
        ]
        
        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Attribute', 'Value'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=11)
                ),
                cells=dict(
                    values=[[s[0] for s in info_data], [s[1] for s in info_data]],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=10)
                )
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title=f"Centrality Profile: {contact_name} - {case_id}",
            height=900,
            showlegend=False
        )
        
        fig.update_yaxes(title_text="Score", row=1, col=1)
        fig.update_yaxes(title_text="Rank (lower is better)", row=1, col=2)
        
        # Save
        output_path = os.path.join(self.output_dir, f'centrality_profile_{case_id}_{contact_digits}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Centrality profile complete: {output_path}")
        
        return output_path
    
    def create_metric_comparison_heatmap(self, case_id, top_n=20):
        """
        Create heatmap comparing all centrality metrics across top contacts
        
        Args:
            case_id: Case identifier
            top_n: Number of top contacts to compare
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🔥 Creating centrality comparison heatmap for {case_id}...")
        
        # Import graph analyzer
        from graph_analytics import ForensicGraphAnalyzer
        
        # Build graph
        analyzer = ForensicGraphAnalyzer(db_path=self.db_path)
        G = analyzer.build_communication_graph(case_id, min_interactions=1)
        
        if G.number_of_nodes() == 0:
            print("   ⚠️  No network data found")
            return None
        
        # Calculate all centrality metrics
        metrics = analyzer.calculate_centrality_metrics(G, top_n=top_n)
        
        # Get contact names
        conn = sqlite3.connect(self.db_path)
        
        # Prepare data
        all_contacts = set()
        for metric_name, values in metrics.items():
            for contact, score in values:
                all_contacts.add(contact)
        
        # Create matrix
        contact_list = list(all_contacts)
        contact_names = []
        
        # Pre-fill default names
        name_map = {contact: contact[:15] for contact in contact_list}
        batch_size = 900

        for i in range(0, len(contact_list), batch_size):
            batch = contact_list[i:i+batch_size]
            placeholders = ','.join(['?'] * len(batch))
            cursor = conn.execute(
                f"SELECT phone_digits, COALESCE(name, phone_raw) FROM contacts WHERE phone_digits IN ({placeholders})",
                batch
            )
            for row in cursor.fetchall():
                phone_digits, name = row
                if phone_digits in name_map:
                    name_map[phone_digits] = name

        for contact in contact_list:
            contact_names.append(name_map[contact])
        
        conn.close()
        
        # Build data matrix
        metric_names = ['degree', 'betweenness', 'closeness', 'pagerank', 'eigenvector']
        data_matrix = []
        
        for metric_key in metric_names:
            row = []
            metric_full = f"{metric_key}_centrality" if metric_key != 'pagerank' else 'pagerank'
            metric_data = dict(metrics.get(metric_full, []))
            
            for contact in contact_list:
                row.append(metric_data.get(contact, 0))
            data_matrix.append(row)
        
        # Normalize each row to 0-1
        normalized_matrix = []
        for row in data_matrix:
            max_val = max(row) if max(row) > 0 else 1
            normalized_matrix.append([v / max_val for v in row])
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=normalized_matrix,
            x=contact_names,
            y=['Degree', 'Betweenness', 'Closeness', 'PageRank', 'Eigenvector'],
            colorscale='Viridis',
            text=[[f"{v:.3f}" for v in row] for row in data_matrix],
            texttemplate='%{text}',
            textfont=dict(size=8),
            colorbar=dict(title="Normalized<br>Score")
        ))
        
        fig.update_layout(
            title=f"Centrality Metrics Comparison Heatmap - {case_id}",
            xaxis_title="Contacts",
            yaxis_title="Centrality Metrics",
            height=600,
            xaxis=dict(tickangle=-45)
        )
        
        # Save
        output_path = os.path.join(self.output_dir, f'centrality_heatmap_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Centrality heatmap complete: {output_path}")
        
        return output_path


# Example usage
if __name__ == "__main__":
    dashboard = CentralityDashboard()
    
    print("="*70)
    print("📊 CENTRALITY DASHBOARD MODULE")
    print("="*70)
    
    # Test with available data
    case_id = 'large_network_case'
    
    # Create visualizations
    dashboard.create_centrality_overview(case_id, top_n=20)
    dashboard.create_metric_comparison_heatmap(case_id, top_n=20)
    
    # Example individual profile (use a contact from the network)
    # dashboard.create_individual_profile(case_id, '1555100001', top_n=20)
    
    print("\n✅ All centrality visualizations created successfully!")
