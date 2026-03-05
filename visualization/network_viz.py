"""
Interactive Network Visualization Module
Creates interactive communication network graphs using PyVis
"""

from pyvis.network import Network
import networkx as nx
import sys
import os
import math

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from visualization.graph_analytics import ForensicGraphAnalyzer

class NetworkVisualizer:
    """
    Creates interactive network visualizations for forensic communication data
    """
    
    def __init__(self, db_path='forensic_data.db'):
        self.analyzer = ForensicGraphAnalyzer(db_path)
        self.output_dir = 'visualization/output'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _generate_uniform_layout(self, num_nodes, canvas_size=8000):
        """
        Generate uniform spatial distribution for nodes like stars in space.
        Uses Fibonacci spiral for optimal uniform distribution.
        
        Args:
            num_nodes: Number of nodes to position
            canvas_size: Size of the canvas space
            
        Returns:
            List of (x, y) positions
        """
        positions = []
        
        # Use Fibonacci sphere mapping to 2D plane for uniform distribution
        golden_ratio = (1 + math.sqrt(5)) / 2
        angle_increment = 2 * math.pi / golden_ratio
        
        for i in range(num_nodes):
            # Fibonacci spiral: ensures uniform distribution
            angle = i * angle_increment
            
            # Radius grows with sqrt to maintain uniform density
            radius = canvas_size * math.sqrt(i / max(num_nodes - 1, 1)) / 2
            
            # Convert to cartesian coordinates
            x = radius * math.cos(angle)
            y = radius * math.sin(angle)
            
            positions.append((x, y))
        
        return positions
    
    def create_communication_network(self, case_id, min_interactions=1, 
                                    color_by='community', size_by='degree',
                                    physics=True, width='100%', height='800px'):
        """
        Create interactive communication network visualization
        
        Args:
            case_id: Case identifier
            min_interactions: Minimum interactions to show edge
            color_by: Node coloring scheme ('community', 'centrality', 'tier')
            size_by: Node sizing scheme ('degree', 'pagerank', 'betweenness')
            physics: Enable physics simulation
            width: Canvas width
            height: Canvas height
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🎨 Creating interactive network visualization for {case_id}...")
        
        # Build graph
        G = self.analyzer.build_communication_graph(case_id, min_interactions=min_interactions)
        
        if G.number_of_nodes() == 0:
            print("   ⚠️ No nodes in graph")
            return None
        
        # Detect communities for coloring
        community_info = self.analyzer.detect_communities(G)
        communities = community_info['node_community']  # Use the node->id map, not the list of lists
        
        # Debug print
        print(f"   DEBUG: communities type: {type(communities)}")
        if not isinstance(communities, dict):
            print(f"   ERROR: communities is not a dict! content: {communities}")
            # Fallback
            if 'node_community' in community_info:
                communities = community_info['node_community']
            else:
                communities = {}

        
        # Calculate centrality for sizing
        print(f"\n   Calculating metrics for visualization...")
        centrality_metrics = self.analyzer.calculate_centrality_metrics(G, top_n=G.number_of_nodes())
        
        # Convert to dicts for quick lookup
        pagerank_dict = dict(centrality_metrics['pagerank'])
        betweenness_dict = dict(centrality_metrics['betweenness_centrality'])
        dict(nx.degree_centrality(G))
        
        # Create PyVis network
        net = Network(width=width, height=height, directed=True, notebook=False)
        
        # For large networks (>500 nodes), use uniform layout instead of physics
        use_uniform_layout = (G.number_of_nodes() > 500) or (not physics)
        
        if use_uniform_layout:
            print(f"   Using uniform spatial distribution for {G.number_of_nodes()} nodes...")
            # Generate uniform positions
            node_positions = self._generate_uniform_layout(
                G.number_of_nodes(),
                canvas_size=8000  # Large canvas for space-like distribution
            )
            node_position_map = dict(zip(list(G.nodes()), node_positions))
            net.toggle_physics(False)
        else:
            print(f"   Using force-directed physics layout...")
            node_position_map = None
            net.toggle_physics(True)
            net.barnes_hut(
                gravity=-50000,
                central_gravity=0.3,
                spring_length=200,
                spring_strength=0.001,
                damping=0.09
            )
        
        # Color palette for communities
        colors = [
            '#e74c3c', '#3498db', '#2ecc71', '#f39c12', 
            '#9b59b6', '#1abc9c', '#34495e', '#e67e22',
            '#95a5a6', '#16a085', '#27ae60', '#2980b9'
        ]
        
        # Calculate in-degree (incoming connections) for sizing
        print(f"   Calculating node importance...")
        in_degree = dict(G.in_degree())
        max_in_degree = max(in_degree.values()) if in_degree else 1
        min_in_degree = min(in_degree.values()) if in_degree else 0
        
        # Add nodes with size based on incoming connections
        print(f"   Adding {G.number_of_nodes()} nodes...")
        for node in G.nodes(data=True):
            node_id = node[0]
            node_data = node[1]
            
            # Determine color by community
            if color_by == 'community':
                color = colors[communities.get(node_id, 0) % len(colors)]
            elif color_by == 'centrality':
                # Color by PageRank (red=high, blue=low)
                pr = pagerank_dict.get(node_id, 0)
                max_pr = max(pagerank_dict.values()) if pagerank_dict else 1
                intensity = int((pr / max_pr) * 255) if max_pr > 0 else 0
                color = f'rgb({intensity}, 0, {255 - intensity})'
            else:
                color = '#3498db'
            
            # Size based on IN-DEGREE (incoming connections)
            node_in_degree = in_degree.get(node_id, 0)
            if max_in_degree > min_in_degree:
                # Normalize to 10-80 range
                normalized = (node_in_degree - min_in_degree) / (max_in_degree - min_in_degree)
                size = 10 + (normalized * 70)  # Size from 10 to 80
            else:
                size = 20
            
            # Create label
            name = node_data.get('name', f"Unknown")
            phone = node_data.get('phone', node_id)
            
            # Community info
            comm_id = communities.get(node_id, 0)
            
            # Get out-degree for display
            out_degree = G.out_degree(node_id)
            
            # Metrics
            pr = pagerank_dict.get(node_id, 0)
            bt = betweenness_dict.get(node_id, 0)
            
            title = f"""
            <b>{name}</b><br>
            Phone: {phone}<br>
            <hr>
            <b>Network Metrics:</b><br>
            Incoming Connections: {node_in_degree}<br>
            Outgoing Connections: {out_degree}<br>
            Community: {comm_id}<br>
            PageRank: {pr:.6f}<br>
            Betweenness: {bt:.6f}<br>
            """
            
            # Add node with optional fixed position for uniform layout
            node_params = {
                'label': name[:20],  # Truncate long names
                'title': title,
                'color': color,
                'size': size,
                'borderWidth': 2,
                'borderWidthSelected': 4
            }
            
            # Apply uniform position if using uniform layout
            if use_uniform_layout and node_position_map:
                x, y = node_position_map[node_id]
                node_params['x'] = x
                node_params['y'] = y
                node_params['physics'] = False  # Lock position
            
            net.add_node(node_id, **node_params)
        
        # Add edges with better visibility
        print(f"   Adding {G.number_of_edges()} edges...")
        for edge in G.edges(data=True):
            source, target, edge_data = edge
            
            # Edge weight (thickness)
            weight = edge_data.get('weight', 1)
            messages = edge_data.get('messages', 0)
            calls = edge_data.get('calls', 0)
            duration = edge_data.get('total_duration', 0)
            
            # Edge width based on total interactions
            width = min(weight / 8, 5)  # Thinner edges
            
            # Edge color based on interaction strength
            if weight > 50:
                edge_color = 'rgba(231, 76, 60, 0.6)'  # Red for strong connections
            elif weight > 20:
                edge_color = 'rgba(243, 156, 18, 0.5)'  # Orange for medium
            else:
                edge_color = 'rgba(149, 165, 166, 0.3)'  # Gray for weak
            
            title = f"""
            {messages} messages, {calls} calls<br>
            Total duration: {duration//60:.0f} minutes<br>
            Interaction weight: {weight}
            """
            
            net.add_edge(
                source,
                target,
                value=width,
                title=title,
                color=edge_color,
                smooth={'enabled': True, 'type': 'continuous'}
            )
        
        # Set options based on layout type
        if use_uniform_layout:
            physics_config = """
            "physics": {
              "enabled": false
            },
            "layout": {
              "randomSeed": 42,
              "improvedLayout": false,
              "hierarchical": false
            }
        """
        else:
            physics_config = """
            "physics": {
              "enabled": true,
              "forceAtlas2Based": {
                "gravitationalConstant": -200,
                "centralGravity": 0.005,
                "springLength": 300,
                "springConstant": 0.02,
                "damping": 0.4,
                "avoidOverlap": 1
              },
              "stabilization": {
                "enabled": true,
                "iterations": 2000,
                "updateInterval": 100,
                "fit": true
              },
              "maxVelocity": 50,
              "minVelocity": 0.75,
              "solver": "forceAtlas2Based",
              "timestep": 0.35,
              "adaptiveTimestep": true
            }
        """
        
        net.set_options(f"""
        {{
          "nodes": {{
            "shape": "dot",
            "font": {{
              "size": 14,
              "face": "arial",
              "strokeWidth": 3,
              "strokeColor": "#ffffff",
              "align": "center"
            }},
            "borderWidth": 2,
            "borderWidthSelected": 4,
            "size": 25,
            "scaling": {{
              "min": 10,
              "max": 80,
              "label": {{
                "enabled": true,
                "min": 14,
                "max": 20
              }}
            }},
            "shadow": {{
              "enabled": true,
              "color": "rgba(0,0,0,0.2)",
              "size": 10,
              "x": 2,
              "y": 2
            }}
          }},
          "edges": {{
            "arrows": {{
              "to": {{
                "enabled": true,
                "scaleFactor": 0.5,
                "type": "arrow"
              }}
            }},
            "smooth": {{
              "enabled": true,
              "type": "continuous",
              "roundness": 0.5
            }},
            "width": 0.5,
            "selectionWidth": 2,
            "hoverWidth": 1.5,
            "length": 300
          }},
          "interaction": {{
            "hover": true,
            "tooltipDelay": 100,
            "zoomView": true,
            "dragView": true,
            "navigationButtons": true,
            "keyboard": true,
            "multiselect": true,
            "zoomSpeed": 1
          }},
          {physics_config}
        }}
        """)
        
        # Save to file
        output_path = os.path.join(self.output_dir, f'network_{case_id}.html')
        net.save_graph(output_path)
        
        print(f"\n   ✅ Network visualization saved: {output_path}")
        print(f"   📊 Visualization details:")
        print(f"      - Nodes: {G.number_of_nodes():,}")
        print(f"      - Edges: {G.number_of_edges():,}")
        print(f"      - Communities: {community_info['num_communities']}")
        print(f"      - Layout: {'Uniform Spatial (star-field)' if use_uniform_layout else 'Force-directed'}")
        print(f"      - Colored by: {color_by}")
        print(f"      - Sized by: in-degree (incoming connections)")
        
        return output_path
    
    def create_ego_network(self, case_id, target_phone, radius=2, **kwargs):
        """
        Create ego network visualization for a specific contact
        
        Args:
            case_id: Case identifier
            target_phone: Target contact phone digits
            radius: Ego network radius (hops)
            **kwargs: Additional visualization parameters
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🎯 Creating ego network for {target_phone}...")
        
        # Build full graph
        G = self.analyzer.build_communication_graph(case_id)
        
        if target_phone not in G.nodes():
            print(f"   ❌ Node {target_phone} not found in graph")
            return None
        
        # Extract ego network
        ego_graph = self.analyzer.get_ego_network(G, target_phone, radius=radius)
        
        # Create visualization with hierarchical layout
        net = Network(width='100%', height='800px', directed=True, notebook=False)
        
        # Use hierarchical layout for better spacing
        net.set_options("""
        {
          "layout": {
            "hierarchical": {
              "enabled": true,
              "levelSeparation": 200,
              "nodeSpacing": 150,
              "treeSpacing": 200,
              "blockShifting": true,
              "edgeMinimization": true,
              "parentCentralization": true,
              "direction": "UD",
              "sortMethod": "directed"
            }
          },
          "physics": {
            "enabled": false
          },
          "nodes": {
            "font": {
              "size": 14,
              "face": "arial"
            }
          },
          "edges": {
            "arrows": {
              "to": {
                "enabled": true,
                "scaleFactor": 0.5
              }
            },
            "smooth": {
              "enabled": true,
              "type": "continuous",
              "roundness": 0.5
            },
            "color": {
              "inherit": false
            }
          },
          "interaction": {
            "hover": true,
            "tooltipDelay": 100,
            "zoomView": true,
            "dragView": true,
            "navigationButtons": true
          }
        }
        """)
        
        # Add nodes with target highlighted
        for node in ego_graph.nodes(data=True):
            node_id = node[0]
            node_data = node[1]
            
            # Highlight target node
            if node_id == target_phone:
                color = '#e74c3c'
                size = 50
                border = 5
                label = f"⭐ {node_data.get('name', 'Target')}"
            else:
                color = '#3498db'
                size = 30
                border = 2
                label = node_data.get('name', 'Unknown')
            
            name = node_data.get('name', 'Unknown')
            phone = node_data.get('phone', node_id)
            
            net.add_node(
                node_id,
                label=label[:30],  # Truncate long labels
                title=f"<b>{name}</b><br>Phone: {phone}<br>Distance: {nx.shortest_path_length(ego_graph, target_phone, node_id) if node_id != target_phone else 0} hops",
                color=color,
                size=size,
                borderWidth=border
            )
        
        # Add edges with better styling
        for source, target, data in ego_graph.edges(data=True):
            weight = data.get('weight', 1)
            messages = data.get('messages', 0)
            calls = data.get('calls', 0)
            
            # Edge color based on connection strength
            if weight > 20:
                edge_color = '#e74c3c'  # Strong connection
            elif weight > 10:
                edge_color = '#f39c12'  # Medium connection
            else:
                edge_color = '#95a5a6'  # Weak connection
            
            net.add_edge(
                source, 
                target, 
                value=min(weight/3, 5),  # Control edge thickness
                title=f"{messages} messages, {calls} calls",
                color=edge_color
            )
        
        # Save
        output_path = os.path.join(self.output_dir, f'ego_network_{case_id}_{target_phone}.html')
        net.save_graph(output_path)
        
        print(f"   ✅ Ego network visualization saved: {output_path}")
        print(f"   📊 Network size: {ego_graph.number_of_nodes()} nodes, {ego_graph.number_of_edges()} edges")
        
        return output_path
    
    def create_community_subgraphs(self, case_id):
        """
        Create separate visualizations for each community
        
        Args:
            case_id: Case identifier
            
        Returns:
            List of paths to generated HTML files
        """
        print(f"\n👥 Creating community subgraph visualizations...")
        
        # Build graph and detect communities
        G = self.analyzer.build_communication_graph(case_id)
        community_info = self.analyzer.detect_communities(G)
        communities = community_info['communities']
        
        # Group nodes by community
        community_nodes = {}
        for node, comm_id in communities.items():
            if comm_id not in community_nodes:
                community_nodes[comm_id] = []
            community_nodes[comm_id].append(node)
        
        output_paths = []
        colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12']
        
        # Create visualization for each community
        for comm_id, nodes in community_nodes.items():
            if len(nodes) < 3:  # Skip very small communities
                continue
            
            print(f"\n   Creating visualization for Community {comm_id} ({len(nodes)} nodes)...")
            
            # Extract subgraph
            subgraph = G.subgraph(nodes)
            
            # Create network
            net = Network(width='100%', height='600px', directed=True, notebook=False)
            net.toggle_physics(True)
            
            color = colors[comm_id % len(colors)]
            
            # Add nodes
            for node in subgraph.nodes(data=True):
                node_id = node[0]
                node_data = node[1]
                name = node_data.get('name', 'Unknown')
                
                net.add_node(
                    node_id,
                    label=name[:15],
                    color=color,
                    size=30
                )
            
            # Add edges
            for source, target, data in subgraph.edges(data=True):
                weight = data.get('weight', 1)
                net.add_edge(source, target, value=weight/5)
            
            # Save
            output_path = os.path.join(
                self.output_dir, 
                f'community_{case_id}_comm{comm_id}.html'
            )
            net.save_graph(output_path)
            output_paths.append(output_path)
        
        print(f"\n   ✅ Created {len(output_paths)} community visualizations")
        
        return output_paths


# Example usage
if __name__ == "__main__":
    visualizer = NetworkVisualizer()
    
    # Create full network visualization
    print("="*70)
    print("🎨 CREATING NETWORK VISUALIZATIONS")
    print("="*70)
    
    # Full network (colored by community, sized by PageRank)
    path = visualizer.create_communication_network(
        'large_network_case',
        min_interactions=2,  # Filter noise
        color_by='community',
        size_by='pagerank',
        physics=True
    )
    
    print(f"\n🌐 Open the visualization in your browser:")
    print(f"   file:///{os.path.abspath(path)}")
    
    # Ego network for boss
    ego_path = visualizer.create_ego_network(
        'large_network_case',
        '1555100001',  # Boss node
        radius=2
    )
    
    print(f"\n🎯 Boss ego network:")
    print(f"   file:///{os.path.abspath(ego_path)}")
    
    # Community subgraphs
    comm_paths = visualizer.create_community_subgraphs('large_network_case')
    
    print(f"\n👥 Community subgraphs created: {len(comm_paths)}")
