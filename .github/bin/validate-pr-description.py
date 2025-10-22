#!/usr/bin/env python3
"""
Validate that PR descriptions contain meaningful content.

This script checks:
1. PR description is not empty
2. Description section (if template is used) has content
3. Ignores Sourcery AI bot summaries
4. Requires minimum meaningful content
"""

import re
import sys


def remove_html_comments(text):
    """Remove HTML comments from text."""
    return re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)


def remove_sourcery_section(text):
    """Remove 'Summary by Sourcery' section."""
    # Match the Sourcery heading and everything until the next heading or end
    pattern = r'##\s+Summary by Sourcery.*?(?=##|$)'
    return re.sub(pattern, '', text, flags=re.DOTALL | re.IGNORECASE)


def extract_description_section(text):
    """
    Extract content from the Description section of the template.
    Returns None if template is not used.
    """
    # Look for "## Description" heading
    match = re.search(r'##\s+Description\s*\n(.*?)(?=##|$)', text, flags=re.DOTALL)
    if match:
        return match.group(1)
    return None


def get_meaningful_content(text):
    """
    Extract meaningful content by removing comments, sourcery, and whitespace.
    """
    # Remove HTML comments
    text = remove_html_comments(text)

    # Remove Sourcery section
    text = remove_sourcery_section(text)

    # Remove markdown code blocks that are empty or just placeholders
    text = re.sub(r'```[a-z]*\s*```', '', text)

    # Remove excessive whitespace
    text = re.sub(r'\n\s*\n', '\n', text)

    return text.strip()


def validate_pr_description(description):
    """
    Validate PR description.
    Returns (is_valid, error_message).
    """
    if not description or not description.strip():
        return False, "PR description is empty. Please provide a description of your changes."

    # Check if template is used
    description_section = extract_description_section(description)

    if description_section is not None:
        # Template is used, validate the Description section
        meaningful_content = get_meaningful_content(description_section)

        if not meaningful_content:
            return False, (
                "PR description's 'Description' section is empty. "
                "Please provide a meaningful description of your changes."
            )

        # Require at least 20 characters of meaningful content
        if len(meaningful_content) < 20:
            return False, (
                f"PR description's 'Description' section is too short ({len(meaningful_content)} chars). "
                "Please provide a more detailed description (at least 20 characters)."
            )
    else:
        # Template not used, validate the entire description
        meaningful_content = get_meaningful_content(description)

        if not meaningful_content:
            return False, (
                "PR description is empty or contains only template comments. "
                "Please provide a meaningful description of your changes."
            )

        # Require at least 50 characters when template is not used
        if len(meaningful_content) < 50:
            return False, (
                f"PR description is too short ({len(meaningful_content)} chars). "
                "Please provide a more detailed description (at least 50 characters)."
            )

    return True, "PR description is valid."


def main():
    if len(sys.argv) < 2:
        print("Usage: validate-pr-description.py <pr_description_file>", file=sys.stderr)
        sys.exit(1)

    description_file = sys.argv[1]

    try:
        with open(description_file, 'r', encoding='utf-8') as f:
            description = f.read()
    except FileNotFoundError:
        print(f"Error: File '{description_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)

    is_valid, message = validate_pr_description(description)

    if is_valid:
        print("✓", message)
        sys.exit(0)
    else:
        print("✗", message, file=sys.stderr)
        print("\nPlease update your PR description to include meaningful information about your changes.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
