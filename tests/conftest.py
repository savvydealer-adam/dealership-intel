"""Shared test fixtures for DealershipIntel."""

import pytest


@pytest.fixture
def sample_contact():
    return {
        "name": "John Smith",
        "title": "General Sales Manager",
        "email": "jsmith@testdealer.com",
        "phone": "(555) 123-4567",
        "linkedin_url": "https://linkedin.com/in/johnsmith",
    }


@pytest.fixture
def sample_company():
    return {
        "domain": "testdealer.com",
        "original_website": "https://www.testdealer.com",
        "company_name": "Test Dealer Auto",
        "industry": "Automotive",
        "company_size": "50",
        "company_phone": "(555) 999-0000",
        "company_address": "123 Main St, Anytown, USA",
    }


@pytest.fixture
def sample_html_staff_page():
    return """
    <html>
    <body>
        <h1>Our Team</h1>
        <div class="staff-member">
            <h3>John Smith</h3>
            <p class="title">General Manager</p>
            <a href="mailto:jsmith@testdealer.com">jsmith@testdealer.com</a>
            <p>(555) 123-4567</p>
        </div>
        <div class="staff-member">
            <h3>Jane Doe</h3>
            <p class="title">Sales Manager</p>
            <a href="mailto:jdoe@testdealer.com">jdoe@testdealer.com</a>
            <p>(555) 234-5678</p>
        </div>
        <div class="staff-member">
            <h3>Bob Wilson</h3>
            <p class="title">Finance Manager</p>
            <a href="mailto:bwilson@testdealer.com">bwilson@testdealer.com</a>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_social_links():
    return """
    <html>
    <body>
        <header>
            <nav>
                <a href="/">Home</a>
                <a href="/inventory">Inventory</a>
            </nav>
        </header>
        <main><h1>Welcome</h1></main>
        <footer>
            <a href="https://www.facebook.com/testdealer">Facebook</a>
            <a href="https://www.instagram.com/testdealer">Instagram</a>
            <a href="https://twitter.com/testdealer">Twitter</a>
            <a href="https://www.youtube.com/channel/UC123">YouTube</a>
            <a href="https://www.linkedin.com/company/testdealer">LinkedIn</a>
        </footer>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_dealeron():
    return """
    <html>
    <head>
        <link rel="stylesheet" href="https://cdn.dealeron.com/assets/style.css">
        <script src="https://cdn.dealeron.com/scripts/main.js"></script>
    </head>
    <body>
        <div class="vehicle-card">Vehicle 1</div>
        <div class="vehicle-card">Vehicle 2</div>
    </body>
    </html>
    """
