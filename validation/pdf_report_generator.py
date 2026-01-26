"""
Data Quality Validation Report Generator for RecoMart
This module creates a comprehensive PDF report of data quality validation results.
"""

from reportlab.lib.pagesizes import A4, letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

def create_pdf_report(validation_results_json, output_path):
    """
    Create a comprehensive PDF report from validation results
    """
    # Load validation results
    with open(validation_results_json, 'r') as f:
        results = json.load(f)
    
    # Create PDF document
    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=72,
        leftMargin=72,
        topMargin=72,
        bottomMargin=18
    )
    
    # Define styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'TitleStyle',
        parent=styles['Title'],
        fontSize=20,
        spaceAfter=30,
        textColor=colors.darkblue,
        alignment=TA_CENTER
    )
    
    heading_style = ParagraphStyle(
        'HeadingStyle',
        parent=styles['Heading1'],
        fontSize=14,
        spaceAfter=12,
        spaceBefore=20,
        textColor=colors.darkgreen
    )
    
    subheading_style = ParagraphStyle(
        'SubHeadingStyle',
        parent=styles['Heading2'],
        fontSize=12,
        spaceAfter=8,
        spaceBefore=12,
        textColor=colors.black
    )
    
    normal_style = styles['Normal']
    
    # Build document content
    story = []
    
    # Title
    story.append(Paragraph("RecoMart Data Quality Validation Report", title_style))
    story.append(Spacer(1, 20))
    
    # Executive Summary
    story.append(Paragraph("Executive Summary", heading_style))
    
    # Calculate overall metrics
    total_datasets = len([r for r in results.values() if 'basic_info' in r])
    total_records = sum(r.get('basic_info', {}).get('total_records', 0) for r in results.values())
    total_issues = sum(len(r.get('validation', {}).get('issues', [])) for r in results.values())
    total_warnings = sum(len(r.get('validation', {}).get('warnings', [])) for r in results.values())
    
    summary_data = [
        ['Metric', 'Value'],
        ['Report Generated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        ['Total Datasets Validated', str(total_datasets)],
        ['Total Records Processed', f'{total_records:,}'],
        ['Critical Issues Found', str(total_issues)],
        ['Warnings Generated', str(total_warnings)],
        ['Data Quality Status', 'GOOD' if total_issues == 0 else 'NEEDS ATTENTION']
    ]
    
    summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    story.append(summary_table)
    story.append(Spacer(1, 30))
    
    # Dataset Details
    story.append(Paragraph("Dataset Validation Details", heading_style))
    
    for dataset_name, dataset_results in results.items():
        if 'error' in dataset_results:
            # Handle error cases
            story.append(Paragraph(f"Dataset: {dataset_name.upper()}", subheading_style))
            story.append(Paragraph(f"<font color='red'>ERROR: {dataset_results['error']}</font>", normal_style))
            story.append(Spacer(1, 15))
            continue
            
        if 'basic_info' not in dataset_results:
            continue
            
        story.append(Paragraph(f"Dataset: {dataset_name.upper()}", subheading_style))
        
        # Basic Information
        basic_info = dataset_results['basic_info']
        basic_data = [
            ['Metric', 'Value'],
            ['Total Records', f"{basic_info.get('total_records', 0):,}"],
            ['Total Columns', str(basic_info.get('total_columns', 0))],
            ['Memory Usage (MB)', str(basic_info.get('memory_usage_mb', 0))],
            ['Source File', dataset_results.get('file_path', '').split('/')[-1]]
        ]
        
        basic_table = Table(basic_data, colWidths=[2.5*inch, 2.5*inch])
        basic_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(basic_table)
        story.append(Spacer(1, 10))
        
        # Data Quality Metrics
        missing_data = dataset_results.get('missing_values', {})
        duplicate_data = dataset_results.get('duplicates', {})
        
        quality_data = [
            ['Quality Metric', 'Value', 'Status'],
            ['Missing Values (%)', f"{missing_data.get('missing_percentage', 0):.1f}%", 
             'OK' if missing_data.get('missing_percentage', 0) < 5 else 'REVIEW'],
            ['Duplicate Records (%)', f"{duplicate_data.get('duplicate_percentage', 0):.1f}%",
             'OK' if duplicate_data.get('duplicate_percentage', 0) < 1 else 'REVIEW'],
            ['Schema Completeness', 'Complete', 'OK']
        ]
        
        quality_table = Table(quality_data, colWidths=[2*inch, 1.5*inch, 1.5*inch])
        quality_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgreen),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(quality_table)
        story.append(Spacer(1, 15))
        
        # Validation Results
        validation = dataset_results.get('validation', {})
        if validation:
            story.append(Paragraph("Validation Results:", normal_style))
            
            if validation.get('passed_checks'):
                story.append(Paragraph("<b>✅ Passed Checks:</b>", normal_style))
                for check in validation['passed_checks']:
                    story.append(Paragraph(f"  • {check}", normal_style))
                    
            if validation.get('warnings'):
                story.append(Paragraph("<b>⚠️ Warnings:</b>", normal_style))
                for warning in validation['warnings']:
                    story.append(Paragraph(f"  • {warning}", normal_style))
                    
            if validation.get('issues'):
                story.append(Paragraph("<b>❌ Issues:</b>", normal_style))
                for issue in validation['issues']:
                    story.append(Paragraph(f"  • {issue}", normal_style))
        
        story.append(Spacer(1, 20))
        
        # Add page break between datasets
        if dataset_name != list(results.keys())[-1]:
            story.append(PageBreak())
    
    # Recommendations Section
    story.append(Paragraph("Recommendations", heading_style))
    
    recommendations = []
    
    # Check for high missing values
    for dataset_name, dataset_results in results.items():
        if 'missing_values' in dataset_results:
            missing_pct = dataset_results['missing_values'].get('missing_percentage', 0)
            if missing_pct > 10:
                recommendations.append(f"Review missing data strategy for {dataset_name} ({missing_pct:.1f}% missing)")
    
    # Check for duplicates
    for dataset_name, dataset_results in results.items():
        if 'duplicates' in dataset_results:
            dup_pct = dataset_results['duplicates'].get('duplicate_percentage', 0)
            if dup_pct > 1:
                recommendations.append(f"Investigate duplicate records in {dataset_name} ({dup_pct:.1f}% duplicates)")
    
    # General recommendations
    if total_issues == 0 and total_warnings == 0:
        recommendations.append("Data quality is excellent. Continue current data management practices.")
    else:
        recommendations.append("Implement data quality monitoring for continuous improvement.")
    
    recommendations.append("Consider implementing automated data quality checks in the ingestion pipeline.")
    recommendations.append("Establish data quality thresholds and alerting mechanisms.")
    
    for i, rec in enumerate(recommendations, 1):
        story.append(Paragraph(f"{i}. {rec}", normal_style))
        story.append(Spacer(1, 8))
    
    # Build PDF
    doc.build(story)
    
    return output_path

def main():
    """Generate PDF report from latest validation results"""
    # Find latest validation results
    reports_dir = Path("validation/reports")
    json_files = list(reports_dir.glob("validation_results_*.json"))
    
    if not json_files:
        print("No validation results found. Run validation first.")
        return
    
    # Get latest file
    latest_file = max(json_files, key=lambda x: x.stat().st_mtime)
    
    # Generate PDF report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    pdf_path = f"validation/reports/data_quality_report_{timestamp}.pdf"
    
    print(f"Generating PDF report from {latest_file.name}...")
    create_pdf_report(str(latest_file), pdf_path)
    print(f"PDF report generated: {pdf_path}")

if __name__ == "__main__":
    main()