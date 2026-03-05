import React, { useEffect, useRef, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import { Button, Card, Select, Input, Tooltip, Spin, message } from 'antd';
import { SearchOutlined, ZoomInOutlined, ZoomOutOutlined, FullscreenOutlined } from '@ant-design/icons';
import axios from 'axios';

const { Option } = Select;

const NODE_COLORS = {
  Person: '#4CAF50',
  Message: '#2196F3',
  Call: '#FFC107',
  Device: '#9C27B0',
  Case: '#F44336',
  CryptoAddress: '#FF5722',
  Media: '#607D8B',
  Location: '#795548',
  Flag: '#E91E63'
};

const KnowledgeGraphViewer = ({ caseId }) => {
  const graphRef = useRef();
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState('');
  const [queryType, setQueryType] = useState('nl');
  const [highlightNodes, setHighlightNodes] = useState(new Set());
  const [highlightLinks, setHighlightLinks] = useState(new Set());
  const [selectedNode, setSelectedNode] = useState(null);

  useEffect(() => {
    if (caseId) {
      loadInitialGraph();
    }
  }, [caseId]);

  const loadInitialGraph = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`/api/graph/case/${caseId}/summary`);
      setGraphData(formatGraphData(response.data));
    } catch (error) {
      message.error('Failed to load graph data');
      console.error('Error loading graph data:', error);
    } finally {
      setLoading(false);
    }
  };

  const formatGraphData = (data) => {
    // Transform API response to format expected by ForceGraph
    const nodes = data.nodes.map(node => ({
      id: node.id,
      label: node.label,
      properties: node.properties,
      color: NODE_COLORS[node.label] || '#999',
      name: node.properties.name || node.properties.case_id || node.id
    }));

    const links = data.relationships.map(rel => ({
      source: rel.from,
      target: rel.to,
      type: rel.type,
      properties: rel.properties
    }));

    return { nodes, links };
  };

  const runQuery = async () => {
    if (!query.trim()) return;
    
    setLoading(true);
    try {
      let endpoint = queryType === 'nl' 
        ? `/api/graph/query/natural?query=${encodeURIComponent(query)}&case_id=${caseId}`
        : `/api/graph/query/cypher`;
      
      let response;
      if (queryType === 'nl') {
        response = await axios.get(endpoint);
      } else {
        response = await axios.post(endpoint, { 
          query: query,
          parameters: { case_id: caseId }
        });
      }
      
      setGraphData(formatGraphData(response.data));
    } catch (error) {
      message.error('Query execution failed');
      console.error('Error executing query:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleNodeClick = (node) => {
    setSelectedNode(node);
    
    // Highlight connected nodes and links
    const connectedNodes = new Set();
    const connectedLinks = new Set();
    
    graphData.links.forEach(link => {
      if (link.source.id === node.id || link.target.id === node.id) {
        connectedNodes.add(link.source);
        connectedNodes.add(link.target);
        connectedLinks.add(link);
      }
    });
    
    setHighlightNodes(connectedNodes);
    setHighlightLinks(connectedLinks);
  };

  const zoomIn = () => {
    if (graphRef.current) {
      const currentZoom = graphRef.current.zoom();
      graphRef.current.zoom(currentZoom * 1.2, 400);
    }
  };

  const zoomOut = () => {
    if (graphRef.current) {
      const currentZoom = graphRef.current.zoom();
      graphRef.current.zoom(currentZoom * 0.8, 400);
    }
  };

  const fitGraph = () => {
    if (graphRef.current) {
      graphRef.current.zoomToFit(400, 40);
    }
  };

  const renderNodeProperties = () => {
    if (!selectedNode) return null;
    
    return (
      <Card title={`${selectedNode.label}: ${selectedNode.name}`} size="small" style={{ marginTop: 16 }}>
        {Object.entries(selectedNode.properties).map(([key, value]) => (
          <div key={key} style={{ marginBottom: 8 }}>
            <strong>{key}:</strong> {typeof value === 'object' ? JSON.stringify(value) : value}
          </div>
        ))}
      </Card>
    );
  };

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div style={{ padding: '16px', display: 'flex', gap: '8px', alignItems: 'center' }}>
        <Select 
          value={queryType} 
          onChange={setQueryType} 
          style={{ width: 120 }}
        >
          <Option value="nl">Natural Language</Option>
          <Option value="cypher">Cypher</Option>
        </Select>
        <Input 
          placeholder={queryType === 'nl' ? "e.g., Find persons connected to crypto addresses" : "MATCH (p:Person)-[r]->(c:CryptoAddress) RETURN p, r, c"}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onPressEnter={runQuery}
          style={{ flex: 1 }}
        />
        <Button type="primary" icon={<SearchOutlined />} onClick={runQuery}>
          Run Query
        </Button>
      </div>
      
      <div style={{ flex: 1, position: 'relative' }}>
        {loading && (
          <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, display: 'flex', justifyContent: 'center', alignItems: 'center', background: 'rgba(255, 255, 255, 0.7)', zIndex: 10 }}>
            <Spin size="large" tip="Loading graph data..." />
          </div>
        )}
        
        <div style={{ position: 'absolute', top: 16, right: 16, zIndex: 5, display: 'flex', flexDirection: 'column', gap: '8px' }}>
          <Tooltip title="Zoom In">
            <Button icon={<ZoomInOutlined />} onClick={zoomIn} />
          </Tooltip>
          <Tooltip title="Zoom Out">
            <Button icon={<ZoomOutOutlined />} onClick={zoomOut} />
          </Tooltip>
          <Tooltip title="Fit Graph">
            <Button icon={<FullscreenOutlined />} onClick={fitGraph} />
          </Tooltip>
        </div>
        
        <ForceGraph2D
          ref={graphRef}
          graphData={graphData}
          nodeLabel={node => `${node.label}: ${node.name}`}
          nodeColor={node => highlightNodes.size ? (highlightNodes.has(node) ? node.color : 'rgba(200, 200, 200, 0.3)') : node.color}
          linkColor={link => highlightLinks.size ? (highlightLinks.has(link) ? '#666' : 'rgba(200, 200, 200, 0.3)') : '#666'}
          linkDirectionalArrowLength={6}
          linkDirectionalArrowRelPos={1}
          linkLabel={link => link.type}
          onNodeClick={handleNodeClick}
          onBackgroundClick={() => {
            setSelectedNode(null);
            setHighlightNodes(new Set());
            setHighlightLinks(new Set());
          }}
          nodeCanvasObject={(node, ctx, globalScale) => {
            const label = node.name;
            const fontSize = 12/globalScale;
            ctx.font = `${fontSize}px Sans-Serif`;
            const textWidth = ctx.measureText(label).width;
            const bckgDimensions = [textWidth, fontSize].map(n => n + fontSize * 0.2);

            ctx.fillStyle = node.color;
            ctx.beginPath();
            ctx.arc(node.x, node.y, 5, 0, 2 * Math.PI, false);
            ctx.fill();

            ctx.fillStyle = 'rgba(255, 255, 255, 0.8)';
            ctx.fillRect(
              node.x - bckgDimensions[0] / 2,
              node.y + 6,
              bckgDimensions[0],
              bckgDimensions[1]
            );

            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillStyle = '#000';
            ctx.fillText(label, node.x, node.y + 6 + fontSize / 2);
          }}
        />
      </div>
      
      {renderNodeProperties()}
    </div>
  );
};

export default KnowledgeGraphViewer;