"""
Communication Pattern Analysis Module
Creates visualizations for call/message patterns, peak hours,
Sankey diagrams, and response time analysis
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import sqlite3
import os
import sys
from collections import defaultdict, Counter

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class CommunicationPatternAnalyzer:
    """Analyzes and visualizes communication patterns"""

    def __init__(self, db_path='forensic_data.db'):
        self.db_path = db_path
        self.output_dir = 'visualization/output'
        os.makedirs(self.output_dir, exist_ok=True)

    def get_communication_data(self, case_id):
        """
        Retrieve all communication data (messages and calls)

        Args:
            case_id: Case identifier

        Returns:
            DataFrames for messages and calls
        """
        conn = sqlite3.connect(self.db_path)

        # Get messages
        messages_df = pd.read_sql_query("""
            SELECT
                msg_id,
                sender_digits,
                receiver_digits,
                timestamp,
                text
            FROM messages
            WHERE case_id = ?
            ORDER BY timestamp
        """, conn, params=(case_id,))

        if not messages_df.empty:
            messages_df['timestamp'] = pd.to_datetime(messages_df['timestamp'])

        # Get calls
        calls_df = pd.read_sql_query("""
            SELECT
                call_id,
                caller_digits,
                receiver_digits,
                timestamp,
                duration_seconds,
                direction
            FROM calls
            WHERE case_id = ?
            ORDER BY timestamp
        """, conn, params=(case_id,))

        if not calls_df.empty:
            calls_df['timestamp'] = pd.to_datetime(calls_df['timestamp'])

        conn.close()

        return messages_df, calls_df

    def create_frequency_chart(self, case_id, time_window='hour'):
        """
        Create call/message frequency chart

        Args:
            case_id: Case identifier
            time_window: Aggregation window ('hour', 'day', 'week')

        Returns:
            Path to generated HTML file
        """
        print(f"\n📊 Creating frequency chart for {case_id} (window: {time_window})...")

        messages_df, calls_df = self.get_communication_data(case_id)

        if messages_df.empty and calls_df.empty:
            print("   ⚠️  No communication data found")
            return None

        # Aggregate by time window
        def aggregate_by_window(df, window):
            if window == 'hour':
                df['time_key'] = df['timestamp'].dt.floor('H')
            elif window == 'day':
                df['time_key'] = df['timestamp'].dt.date
            elif window == 'week':
                df['time_key'] = df['timestamp'].dt.to_period('W').dt.start_time
            return df.groupby('time_key').size()

        # Count messages and calls
        if not messages_df.empty:
            msg_counts = aggregate_by_window(messages_df, time_window)
        else:
            msg_counts = pd.Series()

        if not calls_df.empty:
            call_counts = aggregate_by_window(calls_df, time_window)
        else:
            call_counts = pd.Series()

        # Create figure
        fig = go.Figure()

        if not msg_counts.empty:
            fig.add_trace(go.Scatter(
                x=msg_counts.index,
                y=msg_counts.values,
                mode='lines+markers',
                name='Messages',
                line=dict(color='blue', width=2),
                marker=dict(size=6)
            ))

        if not call_counts.empty:
            fig.add_trace(go.Scatter(
                x=call_counts.index,
                y=call_counts.values,
                mode='lines+markers',
                name='Calls',
                line=dict(color='red', width=2),
                marker=dict(size=6)
            ))

        fig.update_layout(
            title=f"Communication Frequency - {case_id}",
            xaxis_title="Time",
            yaxis_title="Count",
            hovermode='x unified',
            height=600
        )

        # Save
        output_path = os.path.join(self.output_dir, f'frequency_{case_id}_{time_window}.html')
        fig.write_html(output_path)

        print(f"   ✅ Frequency chart saved: {output_path}")
        print(f"   📊 Messages: {len(messages_df):,}, Calls: {len(calls_df):,}")

        return output_path

    def create_peak_hours_heatmap(self, case_id):
        """
        Create peak hours heatmap (hour of day vs day of week)

        Args:
            case_id: Case identifier

        Returns:
            Path to generated HTML file
        """
        print(f"\n🔥 Creating peak hours heatmap for {case_id}...")

        messages_df, calls_df = self.get_communication_data(case_id)

        # Combine all communications
        all_comms = []
        if not messages_df.empty:
            all_comms.extend(messages_df['timestamp'].tolist())
        if not calls_df.empty:
            all_comms.extend(calls_df['timestamp'].tolist())

        if not all_comms:
            print("   ⚠️  No communication data found")
            return None

        # Create hour x day matrix
        hour_day_matrix = np.zeros((24, 7))

        for ts in all_comms:
            hour = ts.hour
            day = ts.dayofweek  # 0=Monday, 6=Sunday
            hour_day_matrix[hour][day] += 1

        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=hour_day_matrix,
            x=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
            y=[f'{h:02d}:00' for h in range(24)],
            colorscale='Hot',
            hoverongaps=False,
            hovertemplate='Day: %{x}<br>Hour: %{y}<br>Count: %{z}<extra></extra>'
        ))

        fig.update_layout(
            title=f"Peak Communication Hours - {case_id}",
            xaxis_title="Day of Week",
            yaxis_title="Hour of Day",
            height=700
        )

        # Save
        output_path = os.path.join(self.output_dir, f'peak_hours_{case_id}.html')
        fig.write_html(output_path)

        print(f"   ✅ Peak hours heatmap saved: {output_path}")
        print(f"   📊 Total communications: {len(all_comms):,}")

        # Find peak hour and day
        peak_hour, peak_day = np.unravel_index(hour_day_matrix.argmax(), hour_day_matrix.shape)
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        print(f"   🔥 Peak: {days[peak_day]} at {peak_hour:02d}:00 ({int(hour_day_matrix[peak_hour][peak_day])} comms)")

        return output_path

    def create_sankey_diagram(self, case_id, top_n=15):
        """
        Create Sankey diagram showing communication flow

        Args:
            case_id: Case identifier
            top_n: Number of top contacts to include

        Returns:
            Path to generated HTML file
        """
        print(f"\n🌊 Creating Sankey diagram for {case_id} (top {top_n} contacts)...")

        messages_df, calls_df = self.get_communication_data(case_id)

        # Combine sources and targets
        edges = []

        if not messages_df.empty:
            # ⚡ Bolt Optimization: Replace iterrows() with itertuples() for 10-50x faster iteration
            for row in messages_df.itertuples(index=False):
                edges.append((row.sender_digits, row.receiver_digits, 'message'))

        if not calls_df.empty:
            # ⚡ Bolt Optimization: Replace iterrows() with itertuples() for 10-50x faster iteration
            for row in calls_df.itertuples(index=False):
                edges.append((row.caller_digits, row.receiver_digits, 'call'))

        if not edges:
            print("   ⚠️  No communication data found")
            return None

        # Count flows
        flow_counts = Counter()
        for source, target, comm_type in edges:
            flow_counts[(source, target)] += 1

        # Get top contacts by total communication volume
        contact_totals = defaultdict(int)
        for (source, target), count in flow_counts.items():
            contact_totals[source] += count
            contact_totals[target] += count

        # Select top N contacts by volume
        top_contacts = [contact for contact, _ in
                       sorted(contact_totals.items(), key=lambda x: x[1], reverse=True)[:top_n]]
        all_contacts = set(top_contacts)

        # Filter flows to top contacts only
        filtered_flows = {k: v for k, v in flow_counts.items()
                         if k[0] in all_contacts and k[1] in all_contacts}

        # Further reduce by keeping only significant flows (top 100)
        filtered_flows = dict(sorted(filtered_flows.items(), key=lambda x: x[1], reverse=True)[:100])

        # Create node list
        nodes = list(all_contacts)
        node_dict = {node: i for i, node in enumerate(nodes)}

        # Create Sankey data
        sources = []
        targets = []
        values = []

        for (source, target), count in filtered_flows.items():
            sources.append(node_dict[source])
            targets.append(node_dict[target])
            values.append(count)

        # Get contact names from database
        conn = sqlite3.connect(self.db_path)
        node_labels = []
        for node in nodes:
            cursor = conn.execute(
                "SELECT COALESCE(name, phone_raw) FROM contacts WHERE phone_digits = ? AND case_id = ? LIMIT 1",
                (node, case_id)
            )
            result = cursor.fetchone()
            label = result[0] if result else str(node)[:15]
            node_labels.append(label)
        conn.close()

        # Create Sankey diagram with optimized settings
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=node_labels,
                color="blue"
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values,
                color="rgba(0, 0, 255, 0.2)"
            )
        )])

        fig.update_layout(
            title=f"Communication Flow (Top {top_n} Contacts) - {case_id}",
            height=800,
            font=dict(size=10)
        )

        # Add warning if diagram is too large
        if len(values) > 200:
            print(f"   ⚠️  Large diagram: {len(values)} flows may cause slow rendering")

        # Save with optimized config to reduce file size
        output_path = os.path.join(self.output_dir, f'sankey_{case_id}.html')

        # Use config to reduce file size
        config = {
            'displayModeBar': True,
            'displaylogo': False,
            'modeBarButtonsToRemove': ['lasso2d', 'select2d']
        }

        # Write with optimized settings
        fig.write_html(
            output_path,
            config=config,
            include_plotlyjs='cdn',  # Use CDN instead of embedding full library
            include_mathjax=False
        )

        print(f"   ✅ Sankey diagram saved: {output_path}")
        print(f"   📊 Nodes: {len(nodes)}, Flows: {len(values)}")

        return output_path

    def create_response_time_analysis(self, case_id):
        """
        Analyze and visualize message response times

        Args:
            case_id: Case identifier

        Returns:
            Path to generated HTML file
        """
        print(f"\n⏱️  Creating response time analysis for {case_id}...")

        messages_df, _ = self.get_communication_data(case_id)

        if messages_df.empty:
            print("   ⚠️  No message data found")
            return None

        # Sort by timestamp
        messages_df = messages_df.sort_values('timestamp')

        # Calculate response times
        response_times = []
        conversation_pairs = defaultdict(list)

        # ⚡ Bolt Optimization: Replace iterrows() with itertuples() for 10-50x faster iteration
        for msg in messages_df.itertuples(index=False):
            sender = msg.sender_digits
            receiver = msg.receiver_digits
            timestamp = msg.timestamp

            # Check if this is a response to a previous message
            pair_key = tuple(sorted([sender, receiver]))

            if conversation_pairs[pair_key]:
                last_msg = conversation_pairs[pair_key][-1]

                # If the receiver becomes sender (response)
                if last_msg['receiver'] == sender:
                    time_diff = (timestamp - last_msg['timestamp']).total_seconds() / 60  # minutes

                    # Only consider responses within 24 hours
                    if 0 < time_diff < 1440:
                        response_times.append({
                            'time_minutes': time_diff,
                            'sender': sender,
                            'receiver': receiver
                        })

            conversation_pairs[pair_key].append({
                'sender': sender,
                'receiver': receiver,
                'timestamp': timestamp
            })

        if not response_times:
            print("   ⚠️  No response time data found")
            return None

        # Create visualization
        response_df = pd.DataFrame(response_times)

        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Response Time Distribution', 'Response Time Over Time',
                           'Quick Responses (<5 min)', 'Response Time Statistics'),
            specs=[[{"type": "histogram"}, {"type": "scatter"}],
                   [{"type": "bar"}, {"type": "table"}]]
        )

        # 1. Histogram of response times
        fig.add_trace(
            go.Histogram(
                x=response_df['time_minutes'],
                nbinsx=50,
                name='Response Times',
                marker_color='skyblue',
                showlegend=False
            ),
            row=1, col=1
        )

        # 2. Response times over time (if we have timestamps)
        # Use index as proxy for time
        fig.add_trace(
            go.Scatter(
                y=response_df['time_minutes'],
                mode='markers',
                marker=dict(size=5, color='orange', opacity=0.6),
                name='Individual Responses',
                showlegend=False
            ),
            row=1, col=2
        )

        # 3. Quick response rate
        quick_threshold = 5  # minutes
        quick_responses = len(response_df[response_df['time_minutes'] <= quick_threshold])
        total_responses = len(response_df)

        fig.add_trace(
            go.Bar(
                x=['Quick (<5min)', 'Slow (>5min)'],
                y=[quick_responses, total_responses - quick_responses],
                marker_color=['green', 'orange'],
                showlegend=False
            ),
            row=2, col=1
        )

        # 4. Statistics table
        stats = [
            ['Total Responses', str(total_responses)],
            ['Avg Response Time', f"{response_df['time_minutes'].mean():.1f} min"],
            ['Median Response Time', f"{response_df['time_minutes'].median():.1f} min"],
            ['Min Response Time', f"{response_df['time_minutes'].min():.1f} min"],
            ['Max Response Time', f"{response_df['time_minutes'].max():.1f} min"],
            ['Quick Response Rate', f"{(quick_responses/total_responses*100):.1f}%"]
        ]

        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Metric', 'Value'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=12)
                ),
                cells=dict(
                    values=[[s[0] for s in stats], [s[1] for s in stats]],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=11)
                )
            ),
            row=2, col=2
        )

        fig.update_layout(
            title=f"Message Response Time Analysis - {case_id}",
            height=800
        )

        # Save
        output_path = os.path.join(self.output_dir, f'response_times_{case_id}.html')
        fig.write_html(output_path)

        print(f"   ✅ Response time analysis saved: {output_path}")
        print(f"   📊 Analyzed {total_responses} responses")
        print(f"   ⚡ Average response time: {response_df['time_minutes'].mean():.1f} minutes")
        print(f"   🚀 Quick response rate: {(quick_responses/total_responses*100):.1f}%")

        return output_path

    def create_call_duration_analysis(self, case_id):
        """
        Analyze call duration patterns

        Args:
            case_id: Case identifier

        Returns:
            Path to generated HTML file
        """
        print(f"\n📞 Creating call duration analysis for {case_id}...")

        _, calls_df = self.get_communication_data(case_id)

        if calls_df.empty:
            print("   ⚠️  No call data found")
            return None

        # Filter out zero-duration calls
        calls_df = calls_df[calls_df['duration_seconds'] > 0]

        if calls_df.empty:
            print("   ⚠️  No valid call duration data found")
            return None

        # Convert to minutes
        calls_df['duration_minutes'] = calls_df['duration_seconds'] / 60

        # Create multi-plot visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Call Duration Distribution', 'Duration by Time of Day',
                           'Duration Categories', 'Duration Statistics'),
            specs=[[{"type": "histogram"}, {"type": "box"}],
                   [{"type": "pie"}, {"type": "table"}]]
        )

        # 1. Duration histogram
        fig.add_trace(
            go.Histogram(
                x=calls_df['duration_minutes'],
                nbinsx=50,
                marker_color='green',
                showlegend=False
            ),
            row=1, col=1
        )

        # 2. Duration by hour of day
        calls_df['hour'] = calls_df['timestamp'].dt.hour
        fig.add_trace(
            go.Box(
                x=calls_df['hour'],
                y=calls_df['duration_minutes'],
                marker_color='blue',
                showlegend=False
            ),
            row=1, col=2
        )

        # 3. Duration categories
        def categorize_duration(minutes):
            if minutes < 1:
                return 'Very Short (<1min)'
            elif minutes < 5:
                return 'Short (1-5min)'
            elif minutes < 15:
                return 'Medium (5-15min)'
            else:
                return 'Long (>15min)'

        calls_df['category'] = calls_df['duration_minutes'].apply(categorize_duration)
        category_counts = calls_df['category'].value_counts()

        fig.add_trace(
            go.Pie(
                labels=category_counts.index,
                values=category_counts.values,
                marker=dict(colors=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99']),
                showlegend=True
            ),
            row=2, col=1
        )

        # 4. Statistics table
        stats = [
            ['Total Calls', str(len(calls_df))],
            ['Avg Duration', f"{calls_df['duration_minutes'].mean():.1f} min"],
            ['Median Duration', f"{calls_df['duration_minutes'].median():.1f} min"],
            ['Total Talk Time', f"{calls_df['duration_minutes'].sum():.1f} min"],
            ['Shortest Call', f"{calls_df['duration_minutes'].min():.1f} min"],
            ['Longest Call', f"{calls_df['duration_minutes'].max():.1f} min"]
        ]

        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Metric', 'Value'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=12)
                ),
                cells=dict(
                    values=[[s[0] for s in stats], [s[1] for s in stats]],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=11)
                )
            ),
            row=2, col=2
        )

        fig.update_layout(
            title=f"Call Duration Analysis - {case_id}",
            height=800
        )

        # Save
        output_path = os.path.join(self.output_dir, f'call_duration_{case_id}.html')
        fig.write_html(output_path)

        print(f"   ✅ Call duration analysis saved: {output_path}")
        print(f"   📊 Analyzed {len(calls_df)} calls")
        print(f"   ⏱️  Average duration: {calls_df['duration_minutes'].mean():.1f} minutes")
        print(f"   📞 Total talk time: {calls_df['duration_minutes'].sum():.1f} minutes")

        return output_path


# Example usage
if __name__ == "__main__":
    analyzer = CommunicationPatternAnalyzer()

    print("="*70)
    print("📞 COMMUNICATION PATTERN ANALYSIS MODULE")
    print("="*70)

    # Test with available data
    case_id = 'large_network_case'

    # Create visualizations
    analyzer.create_frequency_chart(case_id, time_window='day')
    analyzer.create_peak_hours_heatmap(case_id)
    analyzer.create_sankey_diagram(case_id, top_n=15)
    analyzer.create_response_time_analysis(case_id)
    analyzer.create_call_duration_analysis(case_id)

    print("\n✅ All communication pattern visualizations created successfully!")
