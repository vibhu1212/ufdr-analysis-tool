"""
Anomaly Detection Module
Identifies unusual patterns, sudden changes, behavioral anomalies,
and outliers in communication data
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import sqlite3
import os
import sys
from collections import defaultdict
from scipy import stats

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class AnomalyDetector:
    """Detects anomalies and unusual patterns in forensic communication data"""

    def __init__(self, db_path='forensic_data.db'):
        self.db_path = db_path
        self.output_dir = 'visualization/output'
        os.makedirs(self.output_dir, exist_ok=True)

    def get_communication_data(self, case_id):
        """Retrieve communication data for analysis"""
        conn = sqlite3.connect(self.db_path)

        # Get messages with timestamps
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
            messages_df['type'] = 'message'

        # Get calls with timestamps and duration
        calls_df = pd.read_sql_query("""
            SELECT
                call_id,
                caller_digits as sender_digits,
                receiver_digits,
                timestamp,
                duration_seconds
            FROM calls
            WHERE case_id = ?
            ORDER BY timestamp
        """, conn, params=(case_id,))

        if not calls_df.empty:
            calls_df['timestamp'] = pd.to_datetime(calls_df['timestamp'])
            calls_df['type'] = 'call'

        conn.close()

        return messages_df, calls_df

    def detect_communication_spikes(self, case_id, time_window='hour', threshold_std=2.0):
        """
        Detect unusual spikes in communication volume

        Args:
            case_id: Case identifier
            time_window: Aggregation window ('hour', 'day')
            threshold_std: Number of standard deviations for anomaly threshold

        Returns:
            Path to generated HTML file
        """
        print(f"\n📈 Detecting communication spikes for {case_id}...")

        messages_df, calls_df = self.get_communication_data(case_id)

        # Combine all communications (create type column from the already-existing type)
        messages_subset = messages_df[['timestamp']].copy() if not messages_df.empty else pd.DataFrame(columns=['timestamp'])
        calls_subset = calls_df[['timestamp']].copy() if not calls_df.empty else pd.DataFrame(columns=['timestamp'])

        if not messages_subset.empty:
            messages_subset['type'] = 'message'
        if not calls_subset.empty:
            calls_subset['type'] = 'call'

        all_comms = pd.concat([messages_subset, calls_subset], ignore_index=True)

        if all_comms.empty:
            print("   ⚠️  No communication data found")
            return None

        # Aggregate by time window
        if time_window == 'hour':
            all_comms['time_key'] = all_comms['timestamp'].dt.floor('H')
        else:
            all_comms['time_key'] = all_comms['timestamp'].dt.date

        volume = all_comms.groupby('time_key').size()

        # Calculate statistics
        mean_vol = volume.mean()
        std_vol = volume.std()
        threshold = mean_vol + (threshold_std * std_vol)

        # Identify anomalies
        anomalies = volume[volume > threshold]

        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Communication Volume Over Time', 'Volume Distribution',
                           'Detected Spikes', 'Spike Statistics'),
            specs=[[{"secondary_y": False}, {"type": "histogram"}],
                   [{"type": "bar"}, {"type": "table"}]]
        )

        # 1. Volume over time with threshold
        fig.add_trace(
            go.Scatter(
                x=volume.index,
                y=volume.values,
                mode='lines+markers',
                name='Volume',
                line=dict(color='blue', width=2),
                marker=dict(size=4)
            ),
            row=1, col=1
        )

        # Add threshold line
        fig.add_trace(
            go.Scatter(
                x=volume.index,
                y=[threshold] * len(volume),
                mode='lines',
                name=f'Threshold ({threshold_std}σ)',
                line=dict(color='red', dash='dash', width=2)
            ),
            row=1, col=1
        )

        # Mark anomalies
        if not anomalies.empty:
            fig.add_trace(
                go.Scatter(
                    x=anomalies.index,
                    y=anomalies.values,
                    mode='markers',
                    name='Spikes',
                    marker=dict(color='red', size=10, symbol='star')
                ),
                row=1, col=1
            )

        # 2. Volume distribution histogram
        fig.add_trace(
            go.Histogram(
                x=volume.values,
                nbinsx=30,
                marker_color='skyblue',
                showlegend=False
            ),
            row=1, col=2
        )

        # 3. Top spikes bar chart
        if not anomalies.empty:
            top_spikes = anomalies.nlargest(10)
            fig.add_trace(
                go.Bar(
                    x=[str(t) for t in top_spikes.index],
                    y=top_spikes.values,
                    marker_color='red',
                    showlegend=False,
                    text=[f"{int(v)}" for v in top_spikes.values],
                    textposition='outside',
                    width=0.6 if len(top_spikes) > 5 else 0.3  # Adjust bar width based on count
                ),
                row=2, col=1
            )
            # Update axes for better visibility
            fig.update_xaxes(tickangle=-45, row=2, col=1)
            fig.update_yaxes(title_text="Volume", row=2, col=1, rangemode='tozero')
        else:
            # Add placeholder when no spikes
            fig.add_annotation(
                text="No spikes detected<br>with current threshold",
                xref="x3", yref="y3",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=14, color="gray"),
                xanchor='center',
                yanchor='middle'
            )

        # 4. Statistics table
        stats_data = [
            ['Total Time Periods', str(len(volume))],
            ['Mean Volume', f'{mean_vol:.1f}'],
            ['Std Deviation', f'{std_vol:.1f}'],
            ['Threshold', f'{threshold:.1f}'],
            ['Spikes Detected', str(len(anomalies))],
            ['Max Spike', f'{volume.max():.0f}' if len(volume) > 0 else 'N/A']
        ]

        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Metric', 'Value'],
                    fill_color='paleturquoise',
                    align='left'
                ),
                cells=dict(
                    values=[[s[0] for s in stats_data], [s[1] for s in stats_data]],
                    fill_color='lavender',
                    align='left'
                )
            ),
            row=2, col=2
        )

        fig.update_layout(
            title=f"Communication Spike Detection - {case_id}",
            height=800,
            showlegend=True
        )

        # Ensure proper spacing between subplots
        fig.update_xaxes(title_text="Time", row=1, col=1)
        fig.update_yaxes(title_text="Volume", row=1, col=1)
        fig.update_xaxes(title_text="Volume", row=1, col=2)
        fig.update_yaxes(title_text="Frequency", row=1, col=2)

        # Save
        output_path = os.path.join(self.output_dir, f'spikes_{case_id}.html')
        fig.write_html(output_path)

        print(f"   ✅ Spike detection complete: {output_path}")
        print(f"   📊 Found {len(anomalies)} spikes out of {len(volume)} periods")

        return output_path

    def detect_unusual_contacts(self, case_id, min_interactions=5):
        """
        Identify contacts with unusual communication patterns

        Args:
            case_id: Case identifier
            min_interactions: Minimum interactions to consider

        Returns:
            Path to generated HTML file
        """
        print(f"\n👤 Detecting unusual contacts for {case_id}...")

        messages_df, calls_df = self.get_communication_data(case_id)

        # Count interactions per contact
        contact_interactions = defaultdict(lambda: {'messages': 0, 'calls': 0, 'total': 0})

        for _, msg in messages_df.iterrows():
            contact_interactions[msg['sender_digits']]['messages'] += 1
            contact_interactions[msg['sender_digits']]['total'] += 1
            contact_interactions[msg['receiver_digits']]['messages'] += 1
            contact_interactions[msg['receiver_digits']]['total'] += 1

        for _, call in calls_df.iterrows():
            contact_interactions[call['sender_digits']]['calls'] += 1
            contact_interactions[call['sender_digits']]['total'] += 1
            contact_interactions[call['receiver_digits']]['calls'] += 1
            contact_interactions[call['receiver_digits']]['total'] += 1

        # Filter by minimum interactions
        filtered_contacts = {k: v for k, v in contact_interactions.items()
                            if v['total'] >= min_interactions}

        if not filtered_contacts:
            print("   ⚠️  No contacts with sufficient interactions")
            return None

        # Calculate z-scores for total interactions
        totals = np.array([v['total'] for v in filtered_contacts.values()])
        z_scores = stats.zscore(totals)

        # Identify outliers (|z| > 2)
        outlier_threshold = 2.0
        outliers = []

        for (contact, data), z_score in zip(filtered_contacts.items(), z_scores):
            if abs(z_score) > outlier_threshold:
                outliers.append({
                    'contact': contact,
                    'total': data['total'],
                    'messages': data['messages'],
                    'calls': data['calls'],
                    'z_score': z_score,
                    'type': 'High Activity' if z_score > 0 else 'Low Activity'
                })

        # Sort by absolute z-score
        outliers.sort(key=lambda x: abs(x['z_score']), reverse=True)

        # Get contact names
        conn = sqlite3.connect(self.db_path)
        for outlier in outliers[:20]:  # Top 20
            cursor = conn.execute(
                "SELECT COALESCE(name, phone_raw) FROM contacts WHERE phone_digits = ? LIMIT 1",
                (outlier['contact'],)
            )
            result = cursor.fetchone()
            outlier['name'] = result[0] if result else outlier['contact'][:15]
        conn.close()

        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Interaction Distribution', 'Z-Score Distribution',
                           'Top Unusual Contacts', 'Outlier Statistics'),
            specs=[[{"type": "histogram"}, {"type": "histogram"}],
                   [{"type": "bar"}, {"type": "table"}]]
        )

        # 1. Interaction distribution
        fig.add_trace(
            go.Histogram(
                x=totals,
                nbinsx=50,
                marker_color='blue',
                showlegend=False
            ),
            row=1, col=1
        )

        # 2. Z-score distribution
        fig.add_trace(
            go.Histogram(
                x=z_scores,
                nbinsx=30,
                marker_color='orange',
                showlegend=False
            ),
            row=1, col=2
        )

        # Add threshold lines
        fig.add_vline(x=outlier_threshold, line_dash="dash", line_color="red", row=1, col=2)
        fig.add_vline(x=-outlier_threshold, line_dash="dash", line_color="red", row=1, col=2)

        # 3. Top outliers bar chart
        if outliers:
            top_outliers = outliers[:15]
            colors = ['red' if x['z_score'] > 0 else 'blue' for x in top_outliers]

            fig.add_trace(
                go.Bar(
                    x=[x['name'] for x in top_outliers],
                    y=[abs(x['z_score']) for x in top_outliers],
                    marker_color=colors,
                    showlegend=False,
                    text=[f"{x['total']} interactions" for x in top_outliers],
                    textposition='outside',
                    width=0.5  # Fixed bar width for consistency
                ),
                row=2, col=1
            )
            # Update axes for better visibility
            fig.update_xaxes(tickangle=-45, row=2, col=1)
            fig.update_yaxes(title_text="|Z-Score|", row=2, col=1, rangemode='tozero')
        else:
            # Add placeholder when no outliers
            fig.add_annotation(
                text="No unusual contacts detected<br>with current threshold",
                xref="x3", yref="y3",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=14, color="gray"),
                xanchor='center',
                yanchor='middle'
            )

        # 4. Statistics table
        stats_data = [
            ['Total Contacts', str(len(filtered_contacts))],
            ['Mean Interactions', f'{totals.mean():.1f}'],
            ['Median Interactions', f'{np.median(totals):.1f}'],
            ['Std Deviation', f'{totals.std():.1f}'],
            ['Outliers Detected', str(len(outliers))],
            ['High Activity', str(sum(1 for x in outliers if x['z_score'] > 0))],
            ['Low Activity', str(sum(1 for x in outliers if x['z_score'] < 0))]
        ]

        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Metric', 'Value'],
                    fill_color='paleturquoise',
                    align='left'
                ),
                cells=dict(
                    values=[[s[0] for s in stats_data], [s[1] for s in stats_data]],
                    fill_color='lavender',
                    align='left'
                )
            ),
            row=2, col=2
        )

        fig.update_layout(
            title=f"Unusual Contact Detection - {case_id}",
            height=800,
            showlegend=True
        )

        # Ensure proper spacing between subplots
        fig.update_xaxes(title_text="Interactions", row=1, col=1)
        fig.update_yaxes(title_text="Count", row=1, col=1)
        fig.update_xaxes(title_text="Z-Score", row=1, col=2)
        fig.update_yaxes(title_text="Count", row=1, col=2)
        fig.update_xaxes(title_text="Contact", row=2, col=1)

        # Save
        output_path = os.path.join(self.output_dir, f'unusual_contacts_{case_id}.html')
        fig.write_html(output_path)

        print(f"   ✅ Unusual contact detection complete: {output_path}")
        print(f"   📊 Found {len(outliers)} outliers out of {len(filtered_contacts)} contacts")

        return output_path

    def detect_behavioral_changes(self, case_id, window_days=7):
        """
        Detect sudden changes in communication behavior

        Args:
            case_id: Case identifier
            window_days: Size of sliding window for comparison

        Returns:
            Path to generated HTML file
        """
        print(f"\n🔄 Detecting behavioral changes for {case_id} (window: {window_days} days)...")

        messages_df, calls_df = self.get_communication_data(case_id)

        # Combine all communications
        all_comms = pd.concat([
            messages_df[['timestamp', 'sender_digits', 'receiver_digits']],
            calls_df[['timestamp', 'sender_digits', 'receiver_digits']]
        ], ignore_index=True)

        if all_comms.empty:
            print("   ⚠️  No communication data found")
            return None

        # Sort by timestamp
        all_comms = all_comms.sort_values('timestamp')

        # Group by day
        all_comms['date'] = all_comms['timestamp'].dt.date
        daily_volume = all_comms.groupby('date').size()

        # Calculate unique contacts per day
        if not all_comms.empty:
            daily_unique = pd.concat([
                all_comms[['date', 'sender_digits']].rename(columns={'sender_digits': 'contact'}),
                all_comms[['date', 'receiver_digits']].rename(columns={'receiver_digits': 'contact'})
            ]).groupby('date')['contact'].nunique()
        else:
            daily_unique = pd.Series(dtype=int)

        # Calculate rolling statistics
        rolling_vol = daily_volume.rolling(window=window_days, min_periods=1)
        vol_mean = rolling_vol.mean()
        vol_std = rolling_vol.std()

        # Detect changes (volume exceeds mean + 2*std)
        changes = []
        for i in range(len(daily_volume)):
            if i >= window_days:
                current = daily_volume.iloc[i]
                expected = vol_mean.iloc[i-1]
                std = vol_std.iloc[i-1]

                if std > 0:
                    z_score = (current - expected) / std
                    if abs(z_score) > 2.0:
                        changes.append({
                            'date': daily_volume.index[i],
                            'volume': current,
                            'expected': expected,
                            'z_score': z_score,
                            'change_pct': ((current - expected) / expected) * 100
                        })

        # Create visualization
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Daily Volume with Rolling Mean', 'Unique Contacts Per Day',
                           'Detected Changes', 'Change Statistics'),
            specs=[[{"secondary_y": False}, {"type": "scatter"}],
                   [{"type": "bar"}, {"type": "table"}]]
        )

        # 1. Daily volume with rolling mean
        fig.add_trace(
            go.Scatter(
                x=daily_volume.index,
                y=daily_volume.values,
                mode='lines+markers',
                name='Daily Volume',
                line=dict(color='blue', width=1),
                marker=dict(size=4)
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=vol_mean.index,
                y=vol_mean.values,
                mode='lines',
                name=f'{window_days}-day Mean',
                line=dict(color='orange', width=2)
            ),
            row=1, col=1
        )

        # Mark detected changes
        if changes:
            change_dates = [c['date'] for c in changes]
            change_volumes = [c['volume'] for c in changes]

            fig.add_trace(
                go.Scatter(
                    x=change_dates,
                    y=change_volumes,
                    mode='markers',
                    name='Changes',
                    marker=dict(color='red', size=10, symbol='star')
                ),
                row=1, col=1
            )

        # 2. Unique contacts per day
        fig.add_trace(
            go.Scatter(
                x=daily_unique.index,
                y=daily_unique.values,
                mode='lines+markers',
                name='Unique Contacts',
                line=dict(color='green', width=1),
                marker=dict(size=4),
                showlegend=False
            ),
            row=1, col=2
        )

        # 3. Top changes
        if changes:
            top_changes = sorted(changes, key=lambda x: abs(x['z_score']), reverse=True)[:10]
            colors = ['red' if c['z_score'] > 0 else 'blue' for c in top_changes]

            fig.add_trace(
                go.Bar(
                    x=[str(c['date']) for c in top_changes],
                    y=[c['change_pct'] for c in top_changes],
                    marker_color=colors,
                    showlegend=False,
                    text=[f"Vol: {c['volume']}" for c in top_changes],
                    textposition='outside',
                    width=0.5  # Fixed bar width for consistency
                ),
                row=2, col=1
            )
            # Update axes for better visibility
            fig.update_xaxes(tickangle=-45, row=2, col=1)
            fig.update_yaxes(title_text="Change %", row=2, col=1)
        else:
            # Add placeholder when no changes
            fig.add_annotation(
                text="No significant behavioral changes<br>detected with current window",
                xref="x3", yref="y3",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=14, color="gray"),
                xanchor='center',
                yanchor='middle'
            )

        # 4. Statistics table
        stats_data = [
            ['Total Days', str(len(daily_volume))],
            ['Mean Daily Volume', f'{daily_volume.mean():.1f}'],
            ['Std Daily Volume', f'{daily_volume.std():.1f}'],
            ['Changes Detected', str(len(changes))],
            ['Increases', str(sum(1 for c in changes if c['z_score'] > 0))],
            ['Decreases', str(sum(1 for c in changes if c['z_score'] < 0))]
        ]

        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Metric', 'Value'],
                    fill_color='paleturquoise',
                    align='left'
                ),
                cells=dict(
                    values=[[s[0] for s in stats_data], [s[1] for s in stats_data]],
                    fill_color='lavender',
                    align='left'
                )
            ),
            row=2, col=2
        )

        fig.update_layout(
            title=f"Behavioral Change Detection - {case_id}",
            height=800,
            showlegend=True
        )

        # Ensure proper spacing between subplots
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(title_text="Volume", row=1, col=1)
        fig.update_xaxes(title_text="Date", row=1, col=2)
        fig.update_yaxes(title_text="Unique Contacts", row=1, col=2)
        fig.update_xaxes(title_text="Date", row=2, col=1)

        # Save
        output_path = os.path.join(self.output_dir, f'behavioral_changes_{case_id}.html')
        fig.write_html(output_path)

        print(f"   ✅ Behavioral change detection complete: {output_path}")
        print(f"   📊 Found {len(changes)} significant changes")

        return output_path

    def create_anomaly_dashboard(self, case_id):
        """
        Create comprehensive anomaly detection dashboard

        Args:
            case_id: Case identifier

        Returns:
            Path to generated HTML file
        """
        print(f"\n🎯 Creating anomaly dashboard for {case_id}...")

        messages_df, calls_df = self.get_communication_data(case_id)

        # Combine data
        all_comms = pd.concat([
            messages_df[['timestamp', 'sender_digits', 'receiver_digits']],
            calls_df[['timestamp', 'sender_digits', 'receiver_digits']]
        ], ignore_index=True)

        if all_comms.empty:
            print("   ⚠️  No communication data found")
            return None

        # Analysis 1: Time gaps (periods of no communication)
        all_comms = all_comms.sort_values('timestamp')
        time_diffs = all_comms['timestamp'].diff().dt.total_seconds() / 3600  # hours
        large_gaps = time_diffs[time_diffs > 24].sort_values(ascending=False)  # >24 hours

        # Analysis 2: Late night communications (11 PM - 5 AM)
        all_comms['hour'] = all_comms['timestamp'].dt.hour
        late_night = all_comms[(all_comms['hour'] >= 23) | (all_comms['hour'] <= 5)]
        late_night_pct = (len(late_night) / len(all_comms)) * 100

        # Analysis 3: Weekend vs weekday
        all_comms['is_weekend'] = all_comms['timestamp'].dt.dayofweek >= 5
        weekend_pct = (all_comms['is_weekend'].sum() / len(all_comms)) * 100

        # Create dashboard
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=('Hourly Distribution', 'Day of Week Distribution', 'Time Gap Analysis',
                           'Late Night Activity', 'Weekend vs Weekday', 'Summary Statistics'),
            specs=[[{"type": "bar"}, {"type": "bar"}, {"type": "histogram"}],
                   [{"type": "pie"}, {"type": "pie"}, {"type": "table"}]]
        )

        # 1. Hourly distribution
        hourly_dist = all_comms['hour'].value_counts().sort_index()
        colors_hour = ['red' if (h >= 23 or h <= 5) else 'blue' for h in hourly_dist.index]

        fig.add_trace(
            go.Bar(
                x=hourly_dist.index,
                y=hourly_dist.values,
                marker_color=colors_hour,
                showlegend=False
            ),
            row=1, col=1
        )

        # 2. Day of week distribution
        day_dist = all_comms['timestamp'].dt.dayofweek.value_counts().sort_index()
        day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

        fig.add_trace(
            go.Bar(
                x=[day_names[i] for i in day_dist.index],
                y=day_dist.values,
                marker_color='green',
                showlegend=False
            ),
            row=1, col=2
        )

        # 3. Time gap histogram
        fig.add_trace(
            go.Histogram(
                x=time_diffs[time_diffs <= 168],  # Up to 1 week
                nbinsx=50,
                marker_color='purple',
                showlegend=False
            ),
            row=1, col=3
        )

        # 4. Late night vs daytime
        fig.add_trace(
            go.Pie(
                labels=['Daytime', 'Late Night (11PM-5AM)'],
                values=[len(all_comms) - len(late_night), len(late_night)],
                marker=dict(colors=['lightblue', 'darkred']),
                showlegend=True
            ),
            row=2, col=1
        )

        # 5. Weekend vs weekday
        fig.add_trace(
            go.Pie(
                labels=['Weekday', 'Weekend'],
                values=[len(all_comms) - all_comms['is_weekend'].sum(), all_comms['is_weekend'].sum()],
                marker=dict(colors=['lightgreen', 'orange']),
                showlegend=True
            ),
            row=2, col=2
        )

        # 6. Summary statistics
        stats_data = [
            ['Total Communications', f'{len(all_comms):,}'],
            ['Late Night Activity', f'{late_night_pct:.1f}%'],
            ['Weekend Activity', f'{weekend_pct:.1f}%'],
            ['Largest Time Gap', f'{large_gaps.max():.1f} hours' if len(large_gaps) > 0 else 'N/A'],
            ['Gaps > 24h', str(len(large_gaps))],
            ['Avg Time Between', f'{time_diffs.mean():.1f} hours']
        ]

        fig.add_trace(
            go.Table(
                header=dict(
                    values=['Metric', 'Value'],
                    fill_color='paleturquoise',
                    align='left',
                    font=dict(size=11)
                ),
                cells=dict(
                    values=[[s[0] for s in stats_data], [s[1] for s in stats_data]],
                    fill_color='lavender',
                    align='left',
                    font=dict(size=10)
                )
            ),
            row=2, col=3
        )

        fig.update_layout(
            title=f"Anomaly Detection Dashboard - {case_id}",
            height=800,
            showlegend=True
        )

        # Save
        output_path = os.path.join(self.output_dir, f'anomaly_dashboard_{case_id}.html')
        fig.write_html(output_path)

        print(f"   ✅ Anomaly dashboard complete: {output_path}")
        print(f"   📊 Late night: {late_night_pct:.1f}%, Weekend: {weekend_pct:.1f}%")

        return output_path


# Example usage
if __name__ == "__main__":
    detector = AnomalyDetector()

    print("="*70)
    print("🔍 ANOMALY DETECTION MODULE")
    print("="*70)

    # Test with available data
    case_id = 'large_network_case'

    # Run anomaly detection
    detector.detect_communication_spikes(case_id, time_window='day', threshold_std=2.0)
    detector.detect_unusual_contacts(case_id, min_interactions=10)
    detector.detect_behavioral_changes(case_id, window_days=7)
    detector.create_anomaly_dashboard(case_id)

    print("\n✅ All anomaly detection visualizations created successfully!")
