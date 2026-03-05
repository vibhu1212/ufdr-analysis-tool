"""
Timeline Visualization Module
Creates interactive timeline charts using Plotly for temporal analysis
"""

import plotly.graph_objects as go
import sqlite3
from datetime import datetime
from collections import Counter
import pandas as pd
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TimelineVisualizer:
    """
    Creates timeline visualizations for forensic temporal data
    """
    
    def __init__(self, db_path='forensic_data.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.output_dir = 'visualization/output'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def __del__(self):
        """Clean up database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def create_activity_timeline(self, case_id, time_window='day'):
        """
        Create activity timeline showing messages, calls, and locations over time
        
        Args:
            case_id: Case identifier
            time_window: Aggregation window ('hour', 'day', 'week', 'month')
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n📅 Creating activity timeline for {case_id}...")
        
        # Get messages
        self.cursor.execute("""
            SELECT timestamp FROM messages WHERE case_id = ?
        """, (case_id,))
        msg_timestamps = [datetime.fromisoformat(row[0]) for row in self.cursor.fetchall()]
        
        # Get calls
        self.cursor.execute("""
            SELECT timestamp FROM calls WHERE case_id = ?
        """, (case_id,))
        call_timestamps = [datetime.fromisoformat(row[0]) for row in self.cursor.fetchall()]
        
        # Get locations
        self.cursor.execute("""
            SELECT timestamp FROM locations WHERE case_id = ?
        """, (case_id,))
        loc_timestamps = [datetime.fromisoformat(row[0]) for row in self.cursor.fetchall()]
        
        print(f"   📊 Data: {len(msg_timestamps)} messages, {len(call_timestamps)} calls, {len(loc_timestamps)} locations")
        
        # Aggregate by time window
        def aggregate_timestamps(timestamps, window):
            aggregated = Counter()
            for ts in timestamps:
                if window == 'hour':
                    key = ts.replace(minute=0, second=0, microsecond=0)
                elif window == 'day':
                    key = ts.replace(hour=0, minute=0, second=0, microsecond=0)
                elif window == 'week':
                    key = ts - pd.Timedelta(days=ts.weekday())
                    key = key.replace(hour=0, minute=0, second=0, microsecond=0)
                elif window == 'month':
                    key = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                else:
                    key = ts.replace(hour=0, minute=0, second=0, microsecond=0)
                aggregated[key] += 1
            return aggregated
        
        msg_data = aggregate_timestamps(msg_timestamps, time_window)
        call_data = aggregate_timestamps(call_timestamps, time_window)
        loc_data = aggregate_timestamps(loc_timestamps, time_window)
        
        # Get all unique timestamps
        all_times = sorted(set(list(msg_data.keys()) + list(call_data.keys()) + list(loc_data.keys())))
        
        # Create traces
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=all_times,
            y=[msg_data.get(t, 0) for t in all_times],
            mode='lines+markers',
            name='Messages',
            line=dict(color='#3498db', width=2),
            marker=dict(size=6),
            fill='tonexty',
            hovertemplate='%{x}<br>Messages: %{y}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=all_times,
            y=[call_data.get(t, 0) for t in all_times],
            mode='lines+markers',
            name='Calls',
            line=dict(color='#e74c3c', width=2),
            marker=dict(size=6),
            fill='tonexty',
            hovertemplate='%{x}<br>Calls: %{y}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=all_times,
            y=[loc_data.get(t, 0) for t in all_times],
            mode='lines+markers',
            name='Locations',
            line=dict(color='#2ecc71', width=2),
            marker=dict(size=6),
            fill='tonexty',
            hovertemplate='%{x}<br>Locations: %{y}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'Activity Timeline - {case_id}',
            xaxis_title='Time',
            yaxis_title='Event Count',
            hovermode='x unified',
            template='plotly_white',
            height=600,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Save to file
        output_path = os.path.join(self.output_dir, f'timeline_activity_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Activity timeline saved: {output_path}")
        
        return output_path
    
    def create_heatmap_timeline(self, case_id):
        """
        Create heatmap showing activity by day of week and hour of day
        
        Args:
            case_id: Case identifier
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n🔥 Creating activity heatmap for {case_id}...")
        
        # Get all events
        self.cursor.execute("""
            SELECT timestamp FROM messages WHERE case_id = ?
            UNION ALL
            SELECT timestamp FROM calls WHERE case_id = ?
        """, (case_id, case_id))
        
        timestamps = [datetime.fromisoformat(row[0]) for row in self.cursor.fetchall()]
        
        # Create day-hour matrix
        activity_matrix = [[0 for _ in range(24)] for _ in range(7)]
        
        for ts in timestamps:
            day = ts.weekday()  # 0=Monday, 6=Sunday
            hour = ts.hour
            activity_matrix[day][hour] += 1
        
        # Create heatmap
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        hours = [f'{h:02d}:00' for h in range(24)]
        
        fig = go.Figure(data=go.Heatmap(
            z=activity_matrix,
            x=hours,
            y=days,
            colorscale='Blues',
            hovertemplate='%{y}, %{x}<br>Activity: %{z}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'Activity Heatmap - {case_id}<br><sub>Day of Week vs. Hour of Day</sub>',
            xaxis_title='Hour of Day',
            yaxis_title='Day of Week',
            height=500,
            template='plotly_white'
        )
        
        # Save to file
        output_path = os.path.join(self.output_dir, f'timeline_heatmap_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Activity heatmap saved: {output_path}")
        
        return output_path
    
    def create_call_duration_timeline(self, case_id):
        """
        Create timeline showing call duration patterns
        
        Args:
            case_id: Case identifier
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n📞 Creating call duration timeline for {case_id}...")
        
        # Get call data
        self.cursor.execute("""
            SELECT timestamp, duration_seconds
            FROM calls
            WHERE case_id = ?
            ORDER BY timestamp
        """, (case_id,))
        
        data = []
        for row in self.cursor.fetchall():
            ts = datetime.fromisoformat(row[0])
            duration_mins = row[1] / 60 if row[1] else 0
            data.append({'timestamp': ts, 'duration': duration_mins})
        
        if not data:
            print("   ⚠️ No call data found")
            return None
        
        df = pd.DataFrame(data)
        
        # Create scatter plot with size based on duration
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['duration'],
            mode='markers',
            marker=dict(
                size=df['duration']/2,  # Size based on duration
                color=df['duration'],
                colorscale='Reds',
                showscale=True,
                colorbar=dict(title="Duration<br>(minutes)")
            ),
            hovertemplate='%{x}<br>Duration: %{y:.1f} minutes<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'Call Duration Timeline - {case_id}',
            xaxis_title='Time',
            yaxis_title='Call Duration (minutes)',
            template='plotly_white',
            height=600
        )
        
        # Save to file
        output_path = os.path.join(self.output_dir, f'timeline_call_duration_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Call duration timeline saved: {output_path}")
        
        return output_path
    
    def create_contact_activity_timeline(self, case_id, top_n=10):
        """
        Create timeline showing top contacts' activity over time
        
        Args:
            case_id: Case identifier
            top_n: Number of top contacts to show
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n👥 Creating contact activity timeline (top {top_n}) for {case_id}...")
        
        # Get top contacts by activity
        self.cursor.execute("""
            SELECT 
                COALESCE(c.name, c.phone_raw) as contact_name,
                c.phone_digits,
                COUNT(*) as total_activity
            FROM (
                SELECT sender_digits as phone FROM messages WHERE case_id = ?
                UNION ALL
                SELECT receiver_digits as phone FROM messages WHERE case_id = ?
                UNION ALL
                SELECT caller_digits as phone FROM calls WHERE case_id = ?
                UNION ALL
                SELECT receiver_digits as phone FROM calls WHERE case_id = ?
            ) activity
            JOIN contacts c ON activity.phone = c.phone_digits AND c.case_id = ?
            GROUP BY c.phone_digits
            ORDER BY total_activity DESC
            LIMIT ?
        """, (case_id, case_id, case_id, case_id, case_id, top_n))
        
        top_contacts = {row[1]: row[0] for row in self.cursor.fetchall()}
        
        if not top_contacts:
            print("   ⚠️ No contact data found")
            return None
        
        print(f"   📊 Analyzing {len(top_contacts)} top contacts...")
        
        # Get activity for each contact over time
        fig = go.Figure()
        
        for phone_digits, name in top_contacts.items():
            # Get activity timestamps for this contact
            self.cursor.execute("""
                SELECT timestamp FROM (
                    SELECT timestamp FROM messages 
                    WHERE case_id = ? AND (sender_digits = ? OR receiver_digits = ?)
                    UNION ALL
                    SELECT timestamp FROM calls 
                    WHERE case_id = ? AND (caller_digits = ? OR receiver_digits = ?)
                )
                ORDER BY timestamp
            """, (case_id, phone_digits, phone_digits, case_id, phone_digits, phone_digits))
            
            timestamps = [datetime.fromisoformat(row[0]) for row in self.cursor.fetchall()]
            
            # Aggregate by day
            daily_counts = Counter()
            for ts in timestamps:
                key = ts.replace(hour=0, minute=0, second=0, microsecond=0)
                daily_counts[key] += 1
            
            times = sorted(daily_counts.keys())
            counts = [daily_counts[t] for t in times]
            
            fig.add_trace(go.Scatter(
                x=times,
                y=counts,
                mode='lines',
                name=name[:20],  # Truncate long names
                hovertemplate='%{x}<br>' + name + '<br>Activity: %{y}<extra></extra>'
            ))
        
        fig.update_layout(
            title=f'Top {top_n} Contacts Activity Timeline - {case_id}',
            xaxis_title='Time',
            yaxis_title='Daily Activity Count',
            hovermode='x unified',
            template='plotly_white',
            height=700,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=1.01
            )
        )
        
        # Save to file
        output_path = os.path.join(self.output_dir, f'timeline_contacts_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Contact activity timeline saved: {output_path}")
        
        return output_path
    
    def create_cumulative_timeline(self, case_id):
        """
        Create cumulative timeline showing total events over time
        
        Args:
            case_id: Case identifier
            
        Returns:
            Path to generated HTML file
        """
        print(f"\n📈 Creating cumulative timeline for {case_id}...")
        
        # Get all events
        self.cursor.execute("""
            SELECT timestamp, 'message' as type FROM messages WHERE case_id = ?
            UNION ALL
            SELECT timestamp, 'call' FROM calls WHERE case_id = ?
            UNION ALL
            SELECT timestamp, 'location' FROM locations WHERE case_id = ?
            ORDER BY timestamp
        """, (case_id, case_id, case_id))
        
        events = [(datetime.fromisoformat(row[0]), row[1]) for row in self.cursor.fetchall()]
        
        # Calculate cumulative counts
        msg_cumulative = []
        call_cumulative = []
        loc_cumulative = []
        
        msg_count = call_count = loc_count = 0
        
        for ts, event_type in events:
            if event_type == 'message':
                msg_count += 1
            elif event_type == 'call':
                call_count += 1
            elif event_type == 'location':
                loc_count += 1
            
            msg_cumulative.append((ts, msg_count))
            call_cumulative.append((ts, call_count))
            loc_cumulative.append((ts, loc_count))
        
        # Create figure
        fig = go.Figure()
        
        if msg_cumulative:
            times, counts = zip(*msg_cumulative)
            fig.add_trace(go.Scatter(
                x=times, y=counts, mode='lines', name='Messages',
                line=dict(color='#3498db', width=2),
                fill='tonexty'
            ))
        
        if call_cumulative:
            times, counts = zip(*call_cumulative)
            fig.add_trace(go.Scatter(
                x=times, y=counts, mode='lines', name='Calls',
                line=dict(color='#e74c3c', width=2),
                fill='tonexty'
            ))
        
        if loc_cumulative:
            times, counts = zip(*loc_cumulative)
            fig.add_trace(go.Scatter(
                x=times, y=counts, mode='lines', name='Locations',
                line=dict(color='#2ecc71', width=2),
                fill='tonexty'
            ))
        
        fig.update_layout(
            title=f'Cumulative Activity Timeline - {case_id}',
            xaxis_title='Time',
            yaxis_title='Cumulative Count',
            hovermode='x unified',
            template='plotly_white',
            height=600
        )
        
        # Save to file
        output_path = os.path.join(self.output_dir, f'timeline_cumulative_{case_id}.html')
        fig.write_html(output_path)
        
        print(f"   ✅ Cumulative timeline saved: {output_path}")
        
        return output_path


# Example usage
if __name__ == "__main__":
    visualizer = TimelineVisualizer()
    
    print("="*70)
    print("📅 CREATING TIMELINE VISUALIZATIONS")
    print("="*70)
    
    case_id = 'large_network_case'
    
    # Activity timeline
    path1 = visualizer.create_activity_timeline(case_id, time_window='day')
    
    # Heatmap
    path2 = visualizer.create_heatmap_timeline(case_id)
    
    # Call duration
    path3 = visualizer.create_call_duration_timeline(case_id)
    
    # Contact activity
    path4 = visualizer.create_contact_activity_timeline(case_id, top_n=15)
    
    # Cumulative
    path5 = visualizer.create_cumulative_timeline(case_id)
    
    print("\n" + "="*70)
    print("✅ ALL TIMELINE VISUALIZATIONS CREATED!")
    print("="*70)
    print(f"\n📂 Open these files in your browser:")
    print(f"   1. Activity Timeline: file:///{os.path.abspath(path1)}")
    print(f"   2. Heatmap: file:///{os.path.abspath(path2)}")
    print(f"   3. Call Duration: file:///{os.path.abspath(path3)}")
    print(f"   4. Contact Activity: file:///{os.path.abspath(path4)}")
    print(f"   5. Cumulative: file:///{os.path.abspath(path5)}")
