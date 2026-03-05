"""
Advanced Network Visualization Module
Creates advanced network analysis visualizations including hierarchical structures,
shortest paths, bridges, temporal evolution, and network structure analysis
"""

import networkx as nx
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from visualization.graph_analytics import ForensicGraphAnalyzer


class AdvancedNetworkAnalyzer:
    """Creates advanced network analysis visualizations"""
    
    def __init__(self, db_path='forensic_data.db'):
        self.analyzer = ForensicGraphAnalyzer(db_path)
        self.output_dir = 'visualization/output'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def find_shortest_paths(self, G, source, target=None, top_n=10):
        """
        Find shortest paths between nodes
        
        Args:
            G: NetworkX graph
            source: Source node
            target: Target node (if None, finds paths to top_n nodes)
            top_n: Number of paths to return if target is None
            
        Returns:
            Dictionary of paths or single path
        """
        print(f"\n🛤️  Finding shortest paths from {source}...")
        
        if target:
            # Find path between specific nodes
            try:
                path = nx.shortest_path(G, source, target)
                length = len(path) - 1
                print(f"   ✅ Path found: {length} hops")
                return {'path': path, 'length': length}
            except nx.NetworkXNoPath:
                print(f"   ⚠️  No path exists between {source} and {target}")
                return None
        else:
            # Find paths to multiple targets
            try:
                lengths = nx.single_source_shortest_path_length(G, source)
                paths = nx.single_source_shortest_path(G, source)
                
                # Sort by path length and get top_n furthest nodes
                sorted_paths = sorted(lengths.items(), key=lambda x: x[1], reverse=True)[:top_n]
                
                result = []
                for node, length in sorted_paths:
                    if node != source and length > 0:
                        result.append({
                            'target': node,
                            'length': length,
                            'path': paths[node]
                        })
                
                print(f"   ✅ Found {len(result)} paths")
                return result
            except:
                print(f"   ⚠️  Error finding paths")
                return []
    
    def analyze_network_structure(self, G):
        """
        Analyze overall network structure and properties
        
        Args:
            G: NetworkX graph
            
        Returns:
            Dictionary with network statistics
        """
        print(f"\n🔬 Analyzing network structure...")
        
        stats = {
            'nodes': G.number_of_nodes(),
            'edges': G.number_of_edges(),
            'density': nx.density(G),
            'is_connected': nx.is_weakly_connected(G) if G.is_directed() else nx.is_connected(G)
        }
        
        # Average clustering coefficient
        if not G.is_directed():
            stats['avg_clustering'] = nx.average_clustering(G)
        else:
            G_undirected = G.to_undirected()
            stats['avg_clustering'] = nx.average_clustering(G_undirected)
        
        # Connected components
        if G.is_directed():
            weak_components = list(nx.weakly_connected_components(G))
            strong_components = list(nx.strongly_connected_components(G))
            stats['weak_components'] = len(weak_components)
            stats['strong_components'] = len(strong_components)
            stats['largest_wcc_size'] = len(max(weak_components, key=len)) if weak_components else 0
            stats['largest_scc_size'] = len(max(strong_components, key=len)) if strong_components else 0
        else:
            components = list(nx.connected_components(G))
            stats['components'] = len(components)
            stats['largest_component_size'] = len(max(components, key=len)) if components else 0
        
        # Average degree
        degrees = [d for n, d in G.degree()]
        stats['avg_degree'] = sum(degrees) / len(degrees) if degrees else 0
        stats['max_degree'] = max(degrees) if degrees else 0
        
        # Diameter (longest shortest path) - only for connected graphs
        try:
            if stats['is_connected']:
                if G.is_directed():
                    stats['diameter'] = nx.diameter(G.to_undirected())
                else:
                    stats['diameter'] = nx.diameter(G)
            else:
                stats['diameter'] = None
        except:
            stats['diameter'] = None
        
        print(f"   ✅ Structure analysis complete")
        print(f"   📊 Nodes: {stats['nodes']:,}, Edges: {stats['edges']:,}")
        print(f"   📊 Density: {stats['density']:.4f}, Avg Degree: {stats['avg_degree']:.2f}")
        
        return stats
    
    def detect_hierarchical_structure(self, G):
        """
        Detect hierarchical structure in network
        
        Args:
            G: NetworkX graph
            
        Returns:
            Hierarchical layers and statistics
        """
        print(f"\n🏛️  Detecting hierarchical structure...")
        
        # Use in-degree and out-degree to identify hierarchy
        in_degrees = dict(G.in_degree())
        out_degrees = dict(G.out_degree())
        
        # Calculate hierarchy score (out-degree / in-degree ratio)
        hierarchy_scores = {}
        for node in G.nodes():
            in_deg = in_degrees[node]
            out_deg = out_degrees[node]
            
            if in_deg == 0 and out_deg > 0:
                # Pure source (top of hierarchy)
                hierarchy_scores[node] = 1.0
            elif out_deg == 0 and in_deg > 0:
                # Pure sink (bottom of hierarchy)
                hierarchy_scores[node] = 0.0
            elif in_deg > 0:
                # Mixed - calculate ratio
                hierarchy_scores[node] = out_deg / (in_deg + out_deg)
            else:
                hierarchy_scores[node] = 0.5
        
        # Classify into layers
        layers = {
            'top': [],      # High out-degree, low in-degree (leaders)
            'middle': [],   # Balanced (coordinators)
            'bottom': []    # High in-degree, low out-degree (receivers)
        }
        
        for node in G.nodes():
            score = hierarchy_scores[node]
            name = G.nodes[node].get('name', 'Unknown')
            
            if score > 0.7:
                layers['top'].append((node, score, name))
            elif score < 0.3:
                layers['bottom'].append((node, score, name))
            else:
                layers['middle'].append((node, score, name))
        
        # Sort layers by score
        for layer in layers:
            layers[layer].sort(key=lambda x: x[1], reverse=True)
        
        print(f"   ✅ Hierarchy detected")
        print(f"   📊 Top layer: {len(layers['top'])} nodes (leaders)")
        print(f"   📊 Middle layer: {len(layers['middle'])} nodes (coordinators)")
        print(f"   📊 Bottom layer: {len(layers['bottom'])} nodes (receivers)")
        
        return {
            'layers': layers,
            'scores': hierarchy_scores
        }
    
    def analyze_temporal_evolution(self, case_id, time_windows=10):
        """
        Analyze how network evolves over time
        
        Args:
            case_id: Case identifier
            time_windows: Number of time windows to divide data into
            
        Returns:
            List of network snapshots over time
        """
        print(f"\n⏳ Analyzing temporal network evolution ({time_windows} windows)...")
        
        # Get all communications with timestamps
        conn = sqlite3.connect(self.analyzer.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT sender_digits, receiver_digits, timestamp, 'message' as type
            FROM messages
            WHERE case_id = ?
            UNION ALL
            SELECT caller_digits, receiver_digits, timestamp, 'call' as type
            FROM calls
            WHERE case_id = ?
            ORDER BY timestamp
        """, (case_id, case_id))
        
        communications = []
        for row in cursor.fetchall():
            try:
                communications.append({
                    'source': row[0],
                    'target': row[1],
                    'timestamp': datetime.fromisoformat(row[2]),
                    'type': row[3]
                })
            except:
                continue
        
        conn.close()
        
        if not communications:
            print("   ⚠️  No temporal data found")
            return []
        
        # Divide into time windows
        start_time = communications[0]['timestamp']
        end_time = communications[-1]['timestamp']
        total_duration = (end_time - start_time).total_seconds()
        window_duration = total_duration / time_windows
        
        snapshots = []
        for i in range(time_windows):
            window_start = start_time + timedelta(seconds=i * window_duration)
            window_end = start_time + timedelta(seconds=(i + 1) * window_duration)
            
            # Filter communications in this window
            window_comms = [
                c for c in communications
                if window_start <= c['timestamp'] < window_end
            ]
            
            # Build graph for this window
            G_window = nx.DiGraph()
            edge_counts = defaultdict(int)
            
            for comm in window_comms:
                G_window.add_edge(comm['source'], comm['target'])
                edge_counts[(comm['source'], comm['target'])] += 1
            
            # Add edge weights
            for (u, v), count in edge_counts.items():
                G_window[u][v]['weight'] = count
            
            # Calculate metrics for this snapshot
            snapshot = {
                'window': i,
                'start_time': window_start,
                'end_time': window_end,
                'nodes': G_window.number_of_nodes(),
                'edges': G_window.number_of_edges(),
                'density': nx.density(G_window) if G_window.number_of_nodes() > 0 else 0,
                'graph': G_window,
                'communications': len(window_comms)
            }
            
            snapshots.append(snapshot)
        
        print(f"   ✅ Created {len(snapshots)} temporal snapshots")
        print(f"   📊 Time range: {start_time} to {end_time}")
        
        return snapshots
    
    def create_hierarchy_visualization(self, case_id):
        """
        Create hierarchical structure visualization
        
        Args:
            case_id: Case identifier
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🏛️  Creating hierarchy visualization for {case_id}...")
        
        # Build graph
        G = self.analyzer.build_communication_graph(case_id)
        
        if G.number_of_nodes() == 0:
            print("   ⚠️  No graph data found")
            return None
        
        # Detect hierarchy
        hierarchy = self.detect_hierarchical_structure(G)
        
        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Hierarchy Structure', 'Layer Distribution', 
                           'Top Leaders', 'Additional Info'),
            specs=[[{"type": "scatter"}, {"type": "bar"}],
                   [{"type": "table"}, {"type": "scatter"}]]
        )
        
        # 1. Hierarchy scatter plot
        for layer, color in [('top', 'red'), ('middle', 'orange'), ('bottom', 'blue')]:
            nodes_data = hierarchy['layers'][layer]
            if nodes_data:
                x_vals = [i for i in range(len(nodes_data))]
                y_vals = [item[1] for item in nodes_data]  # hierarchy score
                names = [item[2] for item in nodes_data]   # node names
                
                fig.add_trace(
                    go.Scatter(
                        x=x_vals, y=y_vals,
                        mode='markers+text',
                        name=f'{layer.title()} Layer',
                        text=names,
                        textposition='top center',
                        marker=dict(color=color, size=10),
                        hovertemplate='%{text}<br>Hierarchy Score: %{y:.3f}<extra></extra>'
                    ),
                    row=1, col=1
                )
        
        # 2. Layer distribution bar chart
        layer_counts = [len(hierarchy['layers'][layer]) for layer in ['top', 'middle', 'bottom']]
        fig.add_trace(
            go.Bar(
                x=['Top (Leaders)', 'Middle (Coordinators)', 'Bottom (Receivers)'],
                y=layer_counts,
                marker_color=['red', 'orange', 'blue'],
                showlegend=False
            ),
            row=1, col=2
        )
        
        # 3. Top leaders table
        top_leaders = hierarchy['layers']['top'][:10]  # Top 10
        if top_leaders:
            fig.add_trace(
                go.Table(
                    header=dict(
                        values=['Node', 'Name', 'Hierarchy Score'],
                        fill_color='paleturquoise',
                        align='left',
                        font=dict(size=12)
                    ),
                    cells=dict(
                        values=[
                            [str(item[0])[:15] for item in top_leaders],
                            [str(item[2])[:30] for item in top_leaders],
                            [f"{item[1]:.3f}" for item in top_leaders]
                        ],
                        fill_color='lavender',
                        align='left',
                        font=dict(size=11)
                    )
                ),
                row=2, col=1
            )
        else:
            # Add placeholder if no leaders
            fig.add_trace(
                go.Table(
                    header=dict(values=['Info']),
                    cells=dict(values=[['No top leaders found']])
                ),
                row=2, col=1
            )
        
        # Update layout
        fig.update_layout(
            title=f"Hierarchical Network Analysis - {case_id}",
            height=800
        )
        
        # Save
        output_path = os.path.join(self.output_dir, f'hierarchy_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Hierarchy visualization saved: {output_path}")
        
        return output_path
    
    def create_temporal_evolution_plot(self, case_id, time_windows=10):
        """
        Create temporal network evolution visualization
        
        Args:
            case_id: Case identifier
            time_windows: Number of time windows
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n⏳ Creating temporal evolution plot for {case_id}...")
        
        # Analyze temporal evolution
        snapshots = self.analyze_temporal_evolution(case_id, time_windows)
        
        if not snapshots:
            print("   ⚠️  No temporal data found")
            return None
        
        # Extract metrics over time
        windows = [s['window'] for s in snapshots]
        nodes = [s['nodes'] for s in snapshots]
        edges = [s['edges'] for s in snapshots]
        densities = [s['density'] for s in snapshots]
        communications = [s['communications'] for s in snapshots]
        
        # Create multi-plot visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Network Size Over Time', 'Network Density', 
                           'Communication Volume', 'Network Evolution Summary'),
            specs=[[{"secondary_y": True}, {"type": "scatter"}],
                   [{"type": "bar"}, {"type": "scatter"}]]
        )
        
        # 1. Network size (nodes and edges)
        fig.add_trace(
            go.Scatter(x=windows, y=nodes, name='Nodes', 
                      line=dict(color='blue'), mode='lines+markers'),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=windows, y=edges, name='Edges', 
                      line=dict(color='red'), mode='lines+markers'),
            row=1, col=1, secondary_y=True
        )
        
        # 2. Network density
        fig.add_trace(
            go.Scatter(x=windows, y=densities, name='Density',
                      line=dict(color='green'), mode='lines+markers',
                      showlegend=False),
            row=1, col=2
        )
        
        # 3. Communication volume
        fig.add_trace(
            go.Bar(x=windows, y=communications, name='Communications',
                   marker_color='orange', showlegend=False),
            row=2, col=1
        )
        
        # 4. Combined evolution view
        # Normalize values for comparison
        max_nodes = max(nodes) if nodes else 1
        max_edges = max(edges) if edges else 1
        max_comms = max(communications) if communications else 1
        
        fig.add_trace(
            go.Scatter(x=windows, y=[n/max_nodes for n in nodes], 
                      name='Nodes (normalized)', line=dict(color='blue', dash='dot')),
            row=2, col=2
        )
        fig.add_trace(
            go.Scatter(x=windows, y=[e/max_edges for e in edges], 
                      name='Edges (normalized)', line=dict(color='red', dash='dot')),
            row=2, col=2
        )
        fig.add_trace(
            go.Scatter(x=windows, y=[c/max_comms for c in communications], 
                      name='Comms (normalized)', line=dict(color='orange', dash='dot')),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title=f"Temporal Network Evolution - {case_id}",
            height=800
        )
        
        # Add time labels
        time_labels = [s['start_time'].strftime('%Y-%m-%d') for s in snapshots]
        fig.update_xaxes(ticktext=time_labels, tickvals=windows)
        
        # Save
        output_path = os.path.join(self.output_dir, f'temporal_evolution_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Temporal evolution plot saved: {output_path}")
        
        return output_path
    
    def create_shortest_paths_visualization(self, case_id, source_node):
        """
        Create shortest paths visualization
        
        Args:
            case_id: Case identifier
            source_node: Source node for paths
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🛤️  Creating shortest paths visualization from {source_node}...")
        
        # Build graph
        G = self.analyzer.build_communication_graph(case_id)
        
        if source_node not in G.nodes():
            print(f"   ⚠️  Node {source_node} not found in graph")
            return None
        
        # Find paths
        paths_data = self.find_shortest_paths(G, source_node, top_n=20)
        
        if not paths_data:
            print("   ⚠️  No paths found")
            return None
        
        # Create visualization
        fig = go.Figure()
        
        # Add nodes
        pos = nx.spring_layout(G, k=3, iterations=50)
        
        node_x = []
        node_y = []
        node_text = []
        node_colors = []
        
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            name = G.nodes[node].get('name', 'Unknown')
            node_text.append(f"{name}<br>({node})")
            
            # Color based on role
            if node == source_node:
                node_colors.append('red')  # Source
            elif any(node == path['target'] for path in paths_data):
                node_colors.append('orange')  # Target
            else:
                node_colors.append('lightblue')  # Other
        
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            marker=dict(size=15, color=node_colors, line=dict(width=2, color='black')),
            text=node_text,
            textposition="middle center",
            hoverinfo='text',
            showlegend=False
        ))
        
        # Add shortest path edges
        edge_colors = ['red', 'orange', 'yellow', 'green', 'blue', 'purple']
        
        for i, path_data in enumerate(paths_data[:6]):  # Show top 6 paths
            path = path_data['path']
            color = edge_colors[i % len(edge_colors)]
            
            edge_x = []
            edge_y = []
            
            for j in range(len(path) - 1):
                x0, y0 = pos[path[j]]
                x1, y1 = pos[path[j + 1]]
                
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])
            
            fig.add_trace(go.Scatter(
                x=edge_x, y=edge_y,
                mode='lines',
                line=dict(width=3, color=color),
                name=f"Path to {path_data['target']} ({path_data['length']} hops)",
                hoverinfo='name'
            ))
        
        # Update layout
        fig.update_layout(
            title=f"Shortest Paths from {source_node} - {case_id}",
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=700,
            showlegend=True
        )
        
        # Save
        output_path = os.path.join(self.output_dir, f'shortest_paths_{case_id}_{source_node}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Shortest paths visualization saved: {output_path}")
        
        return output_path
    
    def create_bridges_visualization(self, case_id):
        """
        Create network bridges visualization
        
        Args:
            case_id: Case identifier
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🌉 Creating bridges visualization for {case_id}...")
        
        # Build graph
        G = self.analyzer.build_communication_graph(case_id)
        
        if G.number_of_nodes() == 0:
            print("   ⚠️  No graph data found")
            return None
        
        # Identify bridges
        bridges = self.analyzer.identify_bridges(G, top_n=20)
        
        if not bridges:
            print("   ⚠️  No bridges found")
            return None
        
        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Bridge Nodes Ranking', 'Bridge Score Distribution', 
                           'Top Bridge Details', 'Betweenness vs Degree'),
            specs=[[{"type": "bar"}, {"type": "histogram"}],
                   [{"type": "table"}, {"type": "scatter"}]]
        )
        
        # 1. Bridge ranking bar chart
        bridge_nodes = [G.nodes[b[0]].get('name', str(b[0]))[:20] for b in bridges[:15]]
        bridge_scores = [b[1] for b in bridges[:15]]
        
        fig.add_trace(
            go.Bar(
                x=bridge_nodes, y=bridge_scores,
                marker_color='orange',
                name='Bridge Score',
                hovertemplate='Node: %{x}<br>Bridge Score: %{y:.4f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # 2. Bridge score distribution
        all_scores = [b[1] for b in bridges]
        fig.add_trace(
            go.Histogram(
                x=all_scores,
                nbinsx=15,
                marker_color='skyblue',
                showlegend=False
            ),
            row=1, col=2
        )
        
        # 3. Top bridges table
        top_bridges = bridges[:10]
        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Node', 'Bridge Score', 'Betweenness', 'Degree'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=12)
                ),
                cells=dict(
                    values=[
                        [G.nodes[b[0]].get('name', str(b[0]))[:30] for b in top_bridges],
                        [f"{b[1]:.4f}" for b in top_bridges],
                        [f"{b[2]:.4f}" for b in top_bridges],
                        [f"{b[3]}" for b in top_bridges]
                    ],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=11)
                )
            ),
            row=2, col=1
        )
        
        # 4. Betweenness vs Degree scatter
        betweenness_vals = [b[2] for b in bridges[:20]]
        degree_vals = [b[3] for b in bridges[:20]]
        node_names = [G.nodes[b[0]].get('name', str(b[0]))[:20] for b in bridges[:20]]
        
        fig.add_trace(
            go.Scatter(
                x=degree_vals, y=betweenness_vals,
                mode='markers+text',
                text=node_names,
                textposition='top center',
                marker=dict(size=10, color='red'),
                name='Bridge Nodes',
                hovertemplate='%{text}<br>Degree: %{x}<br>Betweenness: %{y:.4f}<extra></extra>'
            ),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title=f"Bridge Nodes Analysis - {case_id}",
            height=800,
            showlegend=False
        )
        
        fig.update_xaxes(title_text="Bridge Score", row=1, col=2)
        fig.update_yaxes(title_text="Count", row=1, col=2)
        fig.update_xaxes(title_text="Degree", row=2, col=2)
        fig.update_yaxes(title_text="Betweenness Centrality", row=2, col=2)
        
        # Save
        output_path = os.path.join(self.output_dir, f'bridges_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Bridges visualization saved: {output_path}")
        
        return output_path
    
    def create_network_structure_dashboard(self, case_id):
        """
        Create comprehensive network structure dashboard
        
        Args:
            case_id: Case identifier
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🔬 Creating network structure dashboard for {case_id}...")
        
        # Build graph and analyze
        G = self.analyzer.build_communication_graph(case_id)
        stats = self.analyze_network_structure(G)
        bridges = self.analyzer.identify_bridges(G, top_n=10)
        cliques = self.analyzer.find_cliques(G, min_size=3)
        
        # Create dashboard
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=('Network Statistics', 'Degree Distribution', 'Bridge Nodes',
                           'Component Sizes', 'Clique Sizes', 'Network Properties'),
            specs=[[{"type": "table"}, {"type": "histogram"}, {"type": "bar"}],
                   [{"type": "pie"}, {"type": "bar"}, {"type": "table"}]],
            vertical_spacing=0.15,
            horizontal_spacing=0.1
        )
        
        # 1. Network statistics table
        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Metric', 'Value'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=12, color='black')
                ),
                cells=dict(
                    values=[
                        ['Nodes', 'Edges', 'Density', 'Avg Degree', 'Max Degree', 'Diameter'],
                        [f"{stats['nodes']:,}", f"{stats['edges']:,}", 
                         f"{stats['density']:.4f}", f"{stats['avg_degree']:.2f}",
                         f"{stats['max_degree']}", f"{stats['diameter'] or 'N/A'}"]
                    ],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=11, color='black')
                )
            ),
            row=1, col=1
        )
        
        # 2. Degree distribution
        degrees = [d for n, d in G.degree()]
        fig.add_trace(
            go.Histogram(x=degrees, nbinsx=20, showlegend=False,
                        marker_color='skyblue'),
            row=1, col=2
        )
        
        # 3. Bridge nodes
        if bridges:
            bridge_names = [G.nodes[bridge[0]].get('name', 'Unknown')[:20] for bridge in bridges[:10]]
            bridge_scores = [bridge[1] for bridge in bridges[:10]]
            
            fig.add_trace(
                go.Bar(x=bridge_names, y=bridge_scores, 
                      marker_color='orange', showlegend=False),
                row=1, col=3
            )
        
        # 4. Component sizes (pie chart)
        if G.is_directed():
            components = list(nx.weakly_connected_components(G))
        else:
            components = list(nx.connected_components(G))
        
        if len(components) > 1:
            comp_sizes = [len(comp) for comp in components]
            fig.add_trace(
                go.Pie(values=comp_sizes, 
                      labels=[f"Component {i+1}" for i in range(len(comp_sizes))],
                      showlegend=False),
                row=2, col=1
            )
        
        # 5. Clique sizes
        if cliques:
            clique_sizes = [len(clique) for clique in cliques[:20]]
            clique_counter = Counter(clique_sizes)
            
            fig.add_trace(
                go.Bar(x=list(clique_counter.keys()), y=list(clique_counter.values()),
                      marker_color='green', showlegend=False),
                row=2, col=2
            )
        
        # 6. Additional properties
        properties = [
            ['Connected', 'Yes' if stats['is_connected'] else 'No'],
            ['Clustering Coeff', f"{stats['avg_clustering']:.4f}"]
        ]
        
        if G.is_directed():
            properties.extend([
                ['Weak Components', str(stats['weak_components'])],
                ['Strong Components', str(stats['strong_components'])]
            ])
        
        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Property', 'Value'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=12, color='black')
                ),
                cells=dict(
                    values=[
                        [prop[0] for prop in properties],
                        [prop[1] for prop in properties]
                    ],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=11, color='black')
                )
            ),
            row=2, col=3
        )
        
        # Update layout
        fig.update_layout(
            title=f"Network Structure Analysis - {case_id}",
            height=800
        )
        
        # Save
        output_path = os.path.join(self.output_dir, f'network_structure_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Network structure dashboard saved: {output_path}")
        
        return output_path
    
    # Alias methods for frontend compatibility
    def create_hierarchical_visualization(self, case_id):
        """Alias for create_hierarchy_visualization"""
        # Call the actual method (without 'al' in hierarchical)
        return self.create_hierarchy_visualization(case_id)
    
    def identify_bridges(self, case_id):
        """Create bridges visualization (frontend-compatible method)"""
        # Call the actual method
        return self.create_bridges_visualization(case_id)
    
    def visualize_shortest_paths(self, case_id):
        """
        Create shortest paths visualization with automatic source node selection
        
        Args:
            case_id: Case identifier
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🛤️  Creating shortest paths visualization for {case_id}...")
        
        # Build graph
        G = self.analyzer.build_communication_graph(case_id)
        
        if G.number_of_nodes() == 0:
            print("   ⚠️  No graph data found")
            return None
        
        # Find node with highest degree centrality as source
        degree_cent = nx.degree_centrality(G)
        source_node = max(degree_cent.items(), key=lambda x: x[1])[0]
        
        print(f"   🎯 Auto-selected source node: {source_node}")
        
        # Call the actual method
        return self.create_shortest_paths_visualization(case_id, source_node)
    
    def create_network_evolution(self, case_id, time_windows=10):
        """Alias for create_temporal_evolution_plot"""
        # Call the actual method
        return self.create_temporal_evolution_plot(case_id, time_windows)


# Example usage
if __name__ == "__main__":
    analyzer = AdvancedNetworkAnalyzer()
    
    print("="*70)
    print("🔬 ADVANCED NETWORK ANALYSIS MODULE")
    print("="*70)
    
    # Test with available data
    case_id = 'test1'
    
    # Create advanced visualizations
    analyzer.create_hierarchy_visualization(case_id)
    analyzer.create_temporal_evolution_plot(case_id, time_windows=10)
    analyzer.create_network_structure_dashboard(case_id)
    
    # Test shortest paths (you'd need to know a valid node ID)
    # analyzer.create_shortest_paths_visualization(case_id, 'valid_node_id')
    
    print("\n✅ All advanced visualizations created successfully!")