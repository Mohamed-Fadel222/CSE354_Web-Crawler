// Dashboard JavaScript
document.addEventListener('DOMContentLoaded', function() {
    // Track if the API is available
    let apiAvailable = true;
    
    // Setup dark mode toggle
    const darkModeToggle = document.getElementById('darkModeToggle');
    if (darkModeToggle) {
        // Check if user prefers dark mode
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            document.documentElement.classList.add('dark');
            darkModeToggle.checked = true;
        }
        
        // Check for saved preference
        if (localStorage.getItem('darkMode') === 'true') {
            document.documentElement.classList.add('dark');
            darkModeToggle.checked = true;
        }
        
        // Handle toggle changes
        darkModeToggle.addEventListener('change', function() {
            if (this.checked) {
                document.documentElement.classList.add('dark');
                localStorage.setItem('darkMode', 'true');
            } else {
                document.documentElement.classList.remove('dark');
                localStorage.setItem('darkMode', 'false');
            }
        });
    }
    
    // Setup real-time queue chart if we're on the dashboard page
    const queueChartCanvas = document.getElementById('queueChart');
    let queueChart = null;
    
    if (queueChartCanvas) {
        // Data structure for the chart
        const chartData = {
            labels: [],
            datasets: [
                {
                    label: 'Queue Size',
                    data: [],
                    borderColor: 'rgba(99, 102, 241, 1)',
                    backgroundColor: 'rgba(99, 102, 241, 0.1)',
                    borderWidth: 2,
                    tension: 0.4
                },
                {
                    label: 'In Progress',
                    data: [],
                    borderColor: 'rgba(139, 92, 246, 1)',
                    backgroundColor: 'rgba(139, 92, 246, 0.1)',
                    borderWidth: 2,
                    tension: 0.4
                }
            ]
        };
        
        // Initialize chart
        queueChart = new Chart(queueChartCanvas, {
            type: 'line',
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        ticks: {
                            color: function() {
                                return document.documentElement.classList.contains('dark') ? 
                                    'rgba(255, 255, 255, 0.7)' : 'rgba(0, 0, 0, 0.7)';
                            }
                        },
                        grid: {
                            color: function() {
                                return document.documentElement.classList.contains('dark') ? 
                                    'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)';
                            }
                        }
                    },
                    y: {
                        beginAtZero: true,
                        ticks: {
                            color: function() {
                                return document.documentElement.classList.contains('dark') ? 
                                    'rgba(255, 255, 255, 0.7)' : 'rgba(0, 0, 0, 0.7)';
                            }
                        },
                        grid: {
                            color: function() {
                                return document.documentElement.classList.contains('dark') ? 
                                    'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)';
                            }
                        }
                    }
                },
                plugins: {
                    legend: {
                        labels: {
                            color: function() {
                                return document.documentElement.classList.contains('dark') ? 
                                    'rgba(255, 255, 255, 0.9)' : 'rgba(0, 0, 0, 0.9)';
                            }
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                },
                interaction: {
                    mode: 'nearest',
                    axis: 'x',
                    intersect: false
                },
                animation: {
                    duration: 750
                }
            }
        });
    }
    
    // Function to fetch and update status
    function fetchStatusUpdates() {
        fetch('/api/status')
            .then(response => {
                if (!response.ok) {
                    throw new Error('API error');
                }
                // API is available, hide error if visible
                apiAvailable = true;
                const apiError = document.getElementById('apiError');
                if (apiError) {
                    apiError.classList.add('hidden');
                }
                return response.json();
            })
            .then(data => {
                // Update status cards
                document.getElementById('queueSize').textContent = data.queue_size;
                document.getElementById('inProgress').textContent = data.in_progress;
                document.getElementById('delayed').textContent = data.delayed;
                document.getElementById('crawledCount').textContent = data.crawled_count;
                
                // Only add to chart if we have a new data point
                if (queueChart && chartData.labels.length === 0 || 
                    (data.queue_size !== chartData.datasets[0].data[chartData.datasets[0].data.length - 1] || 
                     data.in_progress !== chartData.datasets[1].data[chartData.datasets[1].data.length - 1])) {
                    
                    // Format the time
                    const now = new Date();
                    const timeLabel = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`;
                    
                    // Add new data point
                    chartData.labels.push(timeLabel);
                    chartData.datasets[0].data.push(data.queue_size);
                    chartData.datasets[1].data.push(data.in_progress);
                    
                    // Remove old data points if we have too many
                    if (chartData.labels.length > 20) {
                        chartData.labels.shift();
                        chartData.datasets.forEach(dataset => {
                            dataset.data.shift();
                        });
                    }
                    
                    // Update chart
                    queueChart.update();
                }
                
                // Animate the progress icon
                const progressIcon = document.querySelector('.progress-icon');
                if (progressIcon && data.in_progress > 0) {
                    progressIcon.classList.add('animate-spin');
                } else if (progressIcon) {
                    progressIcon.classList.remove('animate-spin');
                }
            })
            .catch(error => {
                console.error('Error fetching status:', error);
                
                // Show API error
                if (apiAvailable) {
                    apiAvailable = false;
                    const apiError = document.getElementById('apiError');
                    if (apiError) {
                        apiError.classList.remove('hidden');
                    }
                }
            });
    }
    
    // Initial fetch
    if (document.getElementById('queueSize')) {
        fetchStatusUpdates();
        
        // Set interval for regular updates
        setInterval(fetchStatusUpdates, 5000);
    }
}); 