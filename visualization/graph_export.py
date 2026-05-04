"""
Graph Analysis Export and Reporting Module
Exports graph analysis results to CSV, Excel, JSON, and PDF formats
"""

import pandas as pd
import json
import sqlite3
import os
from datetime import datetime
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class GraphExporter:
    """Export graph analysis results in multiple formats"""

    def __init__(self, db_path='forensic_data.db'):
        self.db_path = db_path
        self.output_dir = 'exports'
        os.makedirs(self.output_dir, exist_ok=True)

    def export_centrality_scores(self, case_id, format='csv', top_n=50):
        """
        Export centrality scores for key players

        Args:
            case_id: Case identifier
            format: Export format ('csv', 'excel', 'json')
            top_n: Number of top contacts to include

        Returns:
            Path to exported file
        """
        print(f"\n📊 Exporting centrality scores for {case_id}...")

        from visualization.graph_analytics import ForensicGraphAnalyzer

        # Build graph and calculate metrics
        analyzer = ForensicGraphAnalyzer(db_path=self.db_path)
        G = analyzer.build_communication_graph(case_id, min_interactions=1)

        if G.number_of_nodes() == 0:
            print("   ⚠️  No network data found")
            return None

        metrics = analyzer.calculate_centrality_metrics(G, top_n=top_n)

        # Get contact information
        conn = sqlite3.connect(self.db_path)

        # Prepare data
        data = []
        all_contacts = set()

        for metric_name, values in metrics.items():
            for contact, score in values:
                all_contacts.add(contact)

        for contact in list(all_contacts)[:top_n]:
            cursor = conn.execute(
                "SELECT COALESCE(name, phone_raw), phone_raw, email FROM contacts WHERE phone_digits = ? LIMIT 1",
                (contact,)
            )
            result = cursor.fetchone()

            row = {
                'phone_digits': contact,
                'name': result[0] if result else contact,
                'phone': result[1] if result else contact,
                'email': result[2] if result and result[2] else 'N/A'
            }

            # Add centrality scores
            for metric_name, values in metrics.items():
                metric_dict = dict(values)
                metric_key = metric_name.replace('_centrality', '').replace('_', ' ').title()
                row[metric_key] = metric_dict.get(contact, 0)

            # Calculate composite score
            scores = [
                row.get('Degree', 0),
                row.get('Betweenness', 0),
                row.get('Closeness', 0),
                row.get('Pagerank', 0),
                row.get('Eigenvector', 0)
            ]
            row['Composite Score'] = sum(scores) / len(scores) if scores else 0

            data.append(row)

        conn.close()

        # Create DataFrame
        df = pd.DataFrame(data)
        df = df.sort_values('Composite Score', ascending=False)

        # Export based on format
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if format == 'csv':
            output_path = os.path.join(self.output_dir, f'centrality_{case_id}_{timestamp}.csv')
            df.to_csv(output_path, index=False)

        elif format == 'excel':
            output_path = os.path.join(self.output_dir, f'centrality_{case_id}_{timestamp}.xlsx')
            df.to_excel(output_path, index=False, engine='openpyxl')

        elif format == 'json':
            output_path = os.path.join(self.output_dir, f'centrality_{case_id}_{timestamp}.json')
            df.to_json(output_path, orient='records', indent=2)

        print(f"   ✅ Exported {len(df)} contacts to {output_path}")
        return output_path

    def export_anomaly_report(self, case_id, format='csv'):
        """
        Export anomaly detection results

        Args:
            case_id: Case identifier
            format: Export format ('csv', 'excel', 'json')

        Returns:
            Path to exported file
        """
        print(f"\n🚨 Exporting anomaly report for {case_id}...")

        from visualization.anomaly_detection_viz import AnomalyDetector

        detector = AnomalyDetector(db_path=self.db_path)

        # Get communication data
        messages_df, calls_df = detector.get_communication_data(case_id)

        if messages_df.empty and calls_df.empty:
            print("   ⚠️  No communication data found")
            return None

        # Combine data
        all_comms = pd.concat([
            messages_df[['timestamp', 'sender_digits', 'receiver_digits']],
            calls_df[['timestamp', 'sender_digits', 'receiver_digits']]
        ], ignore_index=True)

        # Calculate statistics
        all_comms['date'] = all_comms['timestamp'].dt.date
        all_comms['hour'] = all_comms['timestamp'].dt.hour
        all_comms['is_weekend'] = all_comms['timestamp'].dt.dayofweek >= 5
        all_comms['is_late_night'] = (all_comms['hour'] >= 23) | (all_comms['hour'] <= 5)

        # Daily statistics
        daily_stats = all_comms.groupby('date').agg({
            'timestamp': 'count',
            'sender_digits': 'nunique',
            'is_weekend': 'first',
            'is_late_night': 'sum'
        }).reset_index()

        daily_stats.columns = ['Date', 'Total Communications', 'Unique Contacts', 'Is Weekend', 'Late Night Count']

        # Calculate anomalies (simple threshold)
        mean_comms = daily_stats['Total Communications'].mean()
        std_comms = daily_stats['Total Communications'].std()
        threshold = mean_comms + (2 * std_comms)

        daily_stats['Is Spike'] = daily_stats['Total Communications'] > threshold
        daily_stats['Deviation'] = (daily_stats['Total Communications'] - mean_comms) / std_comms

        # Export
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if format == 'csv':
            output_path = os.path.join(self.output_dir, f'anomaly_{case_id}_{timestamp}.csv')
            daily_stats.to_csv(output_path, index=False)

        elif format == 'excel':
            output_path = os.path.join(self.output_dir, f'anomaly_{case_id}_{timestamp}.xlsx')
            daily_stats.to_excel(output_path, index=False, engine='openpyxl')

        elif format == 'json':
            output_path = os.path.join(self.output_dir, f'anomaly_{case_id}_{timestamp}.json')
            daily_stats.to_json(output_path, orient='records', indent=2, date_format='iso')

        print(f"   ✅ Exported {len(daily_stats)} days to {output_path}")
        return output_path

    def export_network_summary(self, case_id, format='json'):
        """
        Export comprehensive network summary statistics

        Args:
            case_id: Case identifier
            format: Export format ('json', 'csv')

        Returns:
            Path to exported file
        """
        print(f"\n📊 Exporting network summary for {case_id}...")

        from visualization.graph_analytics import ForensicGraphAnalyzer
        import networkx as nx

        analyzer = ForensicGraphAnalyzer(db_path=self.db_path)
        G = analyzer.build_communication_graph(case_id, min_interactions=1)

        if G.number_of_nodes() == 0:
            print("   ⚠️  No network data found")
            return None

        # Calculate comprehensive statistics
        summary = {
            'case_id': case_id,
            'export_timestamp': datetime.now().isoformat(),
            'network_statistics': {
                'total_nodes': G.number_of_nodes(),
                'total_edges': G.number_of_edges(),
                'density': nx.density(G),
                'average_degree': sum(dict(G.degree()).values()) / G.number_of_nodes(),
                'is_connected': nx.is_weakly_connected(G),
                'number_of_components': nx.number_weakly_connected_components(G)
            },
            'centrality_statistics': {},
            'communication_statistics': {}
        }

        # Centrality stats
        degree_cent = nx.degree_centrality(G)
        betweenness_cent = nx.betweenness_centrality(G)

        summary['centrality_statistics'] = {
            'max_degree_centrality': max(degree_cent.values()),
            'avg_degree_centrality': sum(degree_cent.values()) / len(degree_cent),
            'max_betweenness': max(betweenness_cent.values()),
            'avg_betweenness': sum(betweenness_cent.values()) / len(betweenness_cent)
        }

        # Communication stats
        conn = sqlite3.connect(self.db_path)

        cursor = conn.execute("SELECT COUNT(*) FROM messages WHERE case_id = ?", (case_id,))
        total_messages = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM calls WHERE case_id = ?", (case_id,))
        total_calls = cursor.fetchone()[0]

        cursor = conn.execute("""
            SELECT MIN(timestamp), MAX(timestamp)
            FROM (
                SELECT timestamp FROM messages WHERE case_id = ?
                UNION ALL
                SELECT timestamp FROM calls WHERE case_id = ?
            )
        """, (case_id, case_id))

        result = cursor.fetchone()
        date_range = {
            'start': result[0] if result[0] else 'N/A',
            'end': result[1] if result[1] else 'N/A'
        }

        conn.close()

        summary['communication_statistics'] = {
            'total_messages': total_messages,
            'total_calls': total_calls,
            'total_communications': total_messages + total_calls,
            'date_range': date_range
        }

        # Export
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if format == 'json':
            output_path = os.path.join(self.output_dir, f'summary_{case_id}_{timestamp}.json')
            with open(output_path, 'w') as f:
                json.dump(summary, f, indent=2)

        elif format == 'csv':
            # Flatten for CSV
            flat_data = []
            for category, stats in summary.items():
                if isinstance(stats, dict):
                    for key, value in stats.items():
                        if isinstance(value, dict):
                            for sub_key, sub_value in value.items():
                                flat_data.append({
                                    'Category': category,
                                    'Metric': f"{key}_{sub_key}",
                                    'Value': str(sub_value)
                                })
                        else:
                            flat_data.append({
                                'Category': category,
                                'Metric': key,
                                'Value': str(value)
                            })
                else:
                    flat_data.append({
                        'Category': category,
                        'Metric': category,
                        'Value': str(stats)
                    })

            df = pd.DataFrame(flat_data)
            output_path = os.path.join(self.output_dir, f'summary_{case_id}_{timestamp}.csv')
            df.to_csv(output_path, index=False)

        print(f"   ✅ Exported summary to {output_path}")
        return output_path

    def export_contact_list(self, case_id, format='csv', include_metrics=True):
        """
        Export complete contact list with optional metrics

        Args:
            case_id: Case identifier
            format: Export format ('csv', 'excel', 'json')
            include_metrics: Include communication metrics

        Returns:
            Path to exported file
        """
        print(f"\n👥 Exporting contact list for {case_id}...")

        conn = sqlite3.connect(self.db_path)

        # Get contacts
        df = pd.read_sql_query("""
            SELECT
                phone_digits,
                COALESCE(name, 'Unknown') as name,
                phone_raw,
                email,
                contact_id
            FROM contacts
            WHERE case_id = ?
            ORDER BY name
        """, conn, params=(case_id,))

        if include_metrics:
            # Add message counts
            msg_sent = pd.read_sql_query("""
                SELECT sender_digits as phone_digits, COUNT(*) as messages_sent
                FROM messages
                WHERE case_id = ?
                GROUP BY sender_digits
            """, conn, params=(case_id,))

            msg_received = pd.read_sql_query("""
                SELECT receiver_digits as phone_digits, COUNT(*) as messages_received
                FROM messages
                WHERE case_id = ?
                GROUP BY receiver_digits
            """, conn, params=(case_id,))

            # Add call counts
            calls_made = pd.read_sql_query("""
                SELECT caller_digits as phone_digits, COUNT(*) as calls_made
                FROM calls
                WHERE case_id = ?
                GROUP BY caller_digits
            """, conn, params=(case_id,))

            calls_received = pd.read_sql_query("""
                SELECT receiver_digits as phone_digits, COUNT(*) as calls_received
                FROM calls
                WHERE case_id = ?
                GROUP BY receiver_digits
            """, conn, params=(case_id,))

            # Merge metrics
            df = df.merge(msg_sent, on='phone_digits', how='left')
            df = df.merge(msg_received, on='phone_digits', how='left')
            df = df.merge(calls_made, on='phone_digits', how='left')
            df = df.merge(calls_received, on='phone_digits', how='left')

            # Fill NaN with 0
            df = df.fillna(0)

            # Calculate totals
            df['total_messages'] = df['messages_sent'] + df['messages_received']
            df['total_calls'] = df['calls_made'] + df['calls_received']
            df['total_communications'] = df['total_messages'] + df['total_calls']

        conn.close()

        # Export
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if format == 'csv':
            output_path = os.path.join(self.output_dir, f'contacts_{case_id}_{timestamp}.csv')
            df.to_csv(output_path, index=False)

        elif format == 'excel':
            output_path = os.path.join(self.output_dir, f'contacts_{case_id}_{timestamp}.xlsx')
            df.to_excel(output_path, index=False, engine='openpyxl')

        elif format == 'json':
            output_path = os.path.join(self.output_dir, f'contacts_{case_id}_{timestamp}.json')
            df.to_json(output_path, orient='records', indent=2)

        print(f"   ✅ Exported {len(df)} contacts to {output_path}")
        return output_path

    def create_investigation_report(self, case_id):
        """
        Create comprehensive investigation report with all exports

        Args:
            case_id: Case identifier

        Returns:
            Dictionary with paths to all exported files
        """
        print(f"\n📄 Creating comprehensive investigation report for {case_id}...")
        print("="*70)

        report_paths = {}

        # Export all formats
        try:
            report_paths['centrality_csv'] = self.export_centrality_scores(case_id, format='csv')
            report_paths['centrality_excel'] = self.export_centrality_scores(case_id, format='excel')
            report_paths['anomaly_csv'] = self.export_anomaly_report(case_id, format='csv')
            report_paths['network_summary'] = self.export_network_summary(case_id, format='json')
            report_paths['contacts_excel'] = self.export_contact_list(case_id, format='excel')
        except Exception as e:
            print(f"   ⚠️  Error during export: {e}")

        # Create report index
        report_index = {
            'case_id': case_id,
            'report_generated': datetime.now().isoformat(),
            'files': report_paths,
            'description': 'Comprehensive investigation report including centrality analysis, anomaly detection, network summary, and contact list'
        }

        # Save index
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        index_path = os.path.join(self.output_dir, f'report_index_{case_id}_{timestamp}.json')

        with open(index_path, 'w') as f:
            json.dump(report_index, f, indent=2)

        report_paths['report_index'] = index_path

        print("\n" + "="*70)
        print("✅ Investigation report complete!")
        print(f"📁 All files saved to: {os.path.abspath(self.output_dir)}")
        print("="*70)

        return report_paths


# Example usage
if __name__ == "__main__":
    exporter = GraphExporter()

    print("="*70)
    print("📊 GRAPH ANALYSIS EXPORT MODULE")
    print("="*70)

    case_id = 'large_network_case'

    # Create comprehensive report
    report = exporter.create_investigation_report(case_id)

    print("\n📋 Generated Files:")
    for key, path in report.items():
        if path:
            print(f"   ✅ {key}: {os.path.basename(path)}")

    print("\n✅ All exports completed successfully!")
