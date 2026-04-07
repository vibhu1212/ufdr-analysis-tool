/**
 * UFDR Upload Component
 * Provides drag-and-drop interface for uploading UFDR files
 * with real-time progress tracking and status updates
 */

import React, { useState, useCallback, useRef } from 'react';
import { 
  Upload, 
  CheckCircle, 
  XCircle, 
  AlertCircle, 
  Loader, 
  File,
  X
} from 'lucide-react';
import { useDropzone } from 'react-dropzone';

interface UFDRUploadProps {
  caseId: string;
  onUploadComplete?: (jobId: string) => void;
}

interface UploadJob {
  id: string;
  filename: string;
  fileSize: number;
  status: 'uploading' | 'processing' | 'completed' | 'failed';
  progress: number;
  currentStep: string;
  detectedFormat?: string;
  tablesCreated: string[];
  processedRecords: number;
  totalRecords: number;
  errors: string[];
  warnings: string[];
  startTime: string;
  endTime?: string;
}

const UFDRUpload: React.FC<UFDRUploadProps> = ({ caseId, onUploadComplete }) => {
  const [uploadJobs, setUploadJobs] = useState<UploadJob[]>([]);
  const [isValidating, setIsValidating] = useState(false);
  const [validationResult, setValidationResult] = useState<any>(null);
  const pollingIntervals = useRef<Map<string, NodeJS.Timeout>>(new Map());

  // Drag and drop handling
  const onDrop = useCallback(async (acceptedFiles: File[]) => {
    for (const file of acceptedFiles) {
      await uploadFile(file);
    }
  }, [caseId]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/zip': ['.zip'],
      'application/x-zip-compressed': ['.zip'],
      'application/octet-stream': ['.ufdr', '.db', '.sqlite']
    },
    multiple: true
  });

  const uploadFile = async (file: File) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('case_id', caseId);

    // Create initial job entry
    const tempJobId = `temp-${Date.now()}`;
    const newJob: UploadJob = {
      id: tempJobId,
      filename: file.name,
      fileSize: file.size,
      status: 'uploading',
      progress: 0,
      currentStep: 'Uploading file...',
      tablesCreated: [],
      processedRecords: 0,
      totalRecords: 0,
      errors: [],
      warnings: [],
      startTime: new Date().toISOString()
    };

    setUploadJobs(prev => [...prev, newJob]);

    try {
      // Upload file
      const response = await fetch('/api/ufdr/upload', {
        method: 'POST',
        body: formData
      });

      if (!response.ok) {
        throw new Error(`Upload failed: ${response.statusText}`);
      }

      const result = await response.json();
      const jobId = result.job_id;

      // Update job with real ID
      setUploadJobs(prev => 
        prev.map(job => 
          job.id === tempJobId 
            ? { ...job, id: jobId, status: 'processing', progress: 5 }
            : job
        )
      );

      // Start polling for status updates
      startStatusPolling(jobId);

    } catch (error) {
      console.error('Upload error:', error);
      setUploadJobs(prev =>
        prev.map(job =>
          job.id === tempJobId
            ? {
                ...job,
                status: 'failed',
                errors: [error instanceof Error ? error.message : 'Upload failed'],
                endTime: new Date().toISOString()
              }
            : job
        )
      );
    }
  };

  const startStatusPolling = (jobId: string) => {
    // Clear any existing interval
    const existingInterval = pollingIntervals.current.get(jobId);
    if (existingInterval) {
      clearInterval(existingInterval);
    }

    // Poll every 2 seconds
    const interval = setInterval(async () => {
      try {
        const response = await fetch(`/api/ufdr/status/${jobId}`);
        
        if (!response.ok) {
          throw new Error('Failed to fetch status');
        }

        const status = await response.json();

        setUploadJobs(prev =>
          prev.map(job =>
            job.id === jobId
              ? {
                  ...job,
                  status: status.status,
                  progress: status.progress,
                  currentStep: status.current_step,
                  detectedFormat: status.detected_format,
                  tablesCreated: status.tables_created || [],
                  processedRecords: status.processed_records || 0,
                  totalRecords: status.total_records || 0,
                  errors: status.errors || [],
                  warnings: status.warnings || [],
                  endTime: status.end_time
                }
              : job
          )
        );

        // Stop polling if job is completed or failed
        if (status.status === 'completed' || status.status === 'failed') {
          clearInterval(interval);
          pollingIntervals.current.delete(jobId);

          if (status.status === 'completed' && onUploadComplete) {
            onUploadComplete(jobId);
          }
        }
      } catch (error) {
        console.error('Status polling error:', error);
        // Continue polling despite errors
      }
    }, 2000);

    pollingIntervals.current.set(jobId, interval);
  };

  const removeJob = (jobId: string) => {
    // Stop polling
    const interval = pollingIntervals.current.get(jobId);
    if (interval) {
      clearInterval(interval);
      pollingIntervals.current.delete(jobId);
    }

    // Remove from list
    setUploadJobs(prev => prev.filter(job => job.id !== jobId));
  };

  const validateFile = async (file: File) => {
    setIsValidating(true);
    setValidationResult(null);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await fetch('/api/ufdr/validate', {
        method: 'POST',
        body: formData
      });

      if (!response.ok) {
        throw new Error('Validation failed');
      }

      const result = await response.json();
      setValidationResult(result);
    } catch (error) {
      console.error('Validation error:', error);
      setValidationResult({
        error: error instanceof Error ? error.message : 'Validation failed'
      });
    } finally {
      setIsValidating(false);
    }
  };

  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  };

  const getStatusIcon = (status: UploadJob['status']) => {
    switch (status) {
      case 'uploading':
      case 'processing':
        return <Loader className="animate-spin text-blue-500" size={20} />;
      case 'completed':
        return <CheckCircle className="text-green-500" size={20} />;
      case 'failed':
        return <XCircle className="text-red-500" size={20} />;
    }
  };

  const getStatusColor = (status: UploadJob['status']) => {
    switch (status) {
      case 'uploading':
      case 'processing':
        return 'bg-blue-500';
      case 'completed':
        return 'bg-green-500';
      case 'failed':
        return 'bg-red-500';
    }
  };

  return (
    <div className="space-y-6">
      {/* Upload Dropzone */}
      <div
        {...getRootProps()}
        className={`
          border-2 border-dashed rounded-lg p-12 text-center cursor-pointer
          transition-colors duration-200
          ${isDragActive 
            ? 'border-blue-500 bg-blue-50' 
            : 'border-gray-300 hover:border-blue-400 hover:bg-gray-50'
          }
        `}
      >
        <input {...getInputProps()} />
        <Upload className="mx-auto mb-4 text-gray-400" size={48} />
        <h3 className="text-lg font-semibold text-gray-700 mb-2">
          {isDragActive ? 'Drop UFDR files here' : 'Upload UFDR Files'}
        </h3>
        <p className="text-sm text-gray-500">
          Drag and drop UFDR files here, or click to browse
        </p>
        <p className="text-xs text-gray-400 mt-2">
          Supports .zip, .ufdr, .db, and .sqlite files
        </p>
      </div>

      {/* Upload Jobs List */}
      {uploadJobs.length > 0 && (
        <div className="space-y-4">
          <h3 className="text-lg font-semibold text-gray-800">Upload Progress</h3>
          
          {uploadJobs.map(job => (
            <div
              key={job.id}
              className="bg-white rounded-lg shadow-md p-4 border border-gray-200"
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center space-x-3 flex-1">
                  {getStatusIcon(job.status)}
                  <div className="flex-1 min-w-0">
                    <h4 className="text-sm font-medium text-gray-900 truncate">
                      {job.filename}
                    </h4>
                    <p className="text-xs text-gray-500">
                      {formatFileSize(job.fileSize)}
                      {job.detectedFormat && (
                        <span className="ml-2">
                          • Format: <span className="font-medium">{job.detectedFormat}</span>
                        </span>
                      )}
                    </p>
                  </div>
                </div>
                
                {(job.status === 'completed' || job.status === 'failed') && (
                  <button
                    aria-label="Remove job"
                    onClick={() => removeJob(job.id)}
                    className="text-gray-400 hover:text-gray-600 transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 rounded-full p-1"
                  >
                    <X size={18} />
                  </button>
                )}
              </div>

              {/* Progress Bar */}
              <div className="mb-3">
                <div className="flex items-center justify-between mb-1">
                  <span className="text-xs text-gray-600">{job.currentStep}</span>
                  <span className="text-xs font-medium text-gray-700">
                    {Math.round(job.progress)}%
                  </span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
                  <div
                    className={`h-full transition-all duration-300 ${getStatusColor(job.status)}`}
                    style={{ width: `${job.progress}%` }}
                  />
                </div>
              </div>

              {/* Details */}
              {job.status === 'completed' && (
                <div className="grid grid-cols-2 gap-4 pt-3 border-t border-gray-200">
                  <div>
                    <p className="text-xs text-gray-500">Tables Created</p>
                    <p className="text-sm font-medium text-gray-900">
                      {job.tablesCreated.length}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500">Records Processed</p>
                    <p className="text-sm font-medium text-gray-900">
                      {job.processedRecords.toLocaleString()}
                    </p>
                  </div>
                </div>
              )}

              {/* Errors */}
              {job.errors.length > 0 && (
                <div className="mt-3 pt-3 border-t border-red-200">
                  <div className="flex items-start space-x-2">
                    <AlertCircle className="text-red-500 flex-shrink-0 mt-0.5" size={16} />
                    <div className="flex-1">
                      <p className="text-xs font-medium text-red-700 mb-1">Errors:</p>
                      {job.errors.map((error, idx) => (
                        <p key={idx} className="text-xs text-red-600">
                          • {error}
                        </p>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {/* Warnings */}
              {job.warnings.length > 0 && (
                <div className="mt-3 pt-3 border-t border-yellow-200">
                  <div className="flex items-start space-x-2">
                    <AlertCircle className="text-yellow-500 flex-shrink-0 mt-0.5" size={16} />
                    <div className="flex-1">
                      <p className="text-xs font-medium text-yellow-700 mb-1">Warnings:</p>
                      {job.warnings.map((warning, idx) => (
                        <p key={idx} className="text-xs text-yellow-600">
                          • {warning}
                        </p>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              {/* Tables Created */}
              {job.tablesCreated.length > 0 && job.status === 'completed' && (
                <details className="mt-3 pt-3 border-t border-gray-200">
                  <summary className="text-xs font-medium text-gray-700 cursor-pointer hover:text-blue-600">
                    View Tables ({job.tablesCreated.length})
                  </summary>
                  <div className="mt-2 flex flex-wrap gap-1">
                    {job.tablesCreated.map((table, idx) => (
                      <span
                        key={idx}
                        className="inline-block px-2 py-1 text-xs bg-blue-100 text-blue-700 rounded"
                      >
                        {table}
                      </span>
                    ))}
                  </div>
                </details>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Case Information */}
      <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
        <div className="flex items-center space-x-2">
          <File className="text-blue-600" size={20} />
          <div>
            <p className="text-sm font-medium text-blue-900">Current Case</p>
            <p className="text-xs text-blue-700">Case ID: {caseId}</p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default UFDRUpload;
