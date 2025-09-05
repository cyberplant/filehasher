# Publishing to PyPI with GitHub Actions

This guide explains how to set up automatic publishing to PyPI using the GitHub Actions workflows.

## 🚀 Quick Setup

### 1. Enable Trusted Publishing (Recommended)

Trusted publishing uses OpenID Connect (OIDC) and doesn't require storing API tokens.

#### For PyPI:
1. Go to [PyPI](https://pypi.org/) → Your Account → Publishing
2. Click "Add a new publisher"
3. Select "GitHub" as the publisher
4. Fill in:
   - Owner: `your-username`
   - Repository: `your-repo-name`
   - Workflow name: `publish.yml`
   - Environment name: `pypi`

#### For TestPyPI:
1. Go to [TestPyPI](https://test.pypi.org/) → Your Account → Publishing
2. Follow the same steps as PyPI

### 2. Create Repository Environments

In your GitHub repository:

1. Go to Settings → Environments
2. Create `pypi` environment:
   - Name: `pypi`
   - URL: `https://pypi.org/p/filehasher`
3. Create `testpypi` environment:
   - Name: `testpypi`
   - URL: `https://test.pypi.org/p/filehasher`

### 3. Configure Branch Protection (Optional)

To prevent accidental publishing:

1. Go to Settings → Branches
2. Add rule for `main`/`master` branch
3. Require status checks to pass
4. Include administrators

## 📋 Workflow Details

### CI Workflow (`ci.yml`)
- **Triggers**: Push/PR to main branches
- **Tests**: Multiple Python versions (3.8-3.12)
- **Linting**: flake8 for code quality
- **Features tested**:
  - Basic functionality
  - Parallel processing
  - All hash algorithms
  - Benchmarking

### Publish Workflow (`publish.yml`)
- **Triggers**:
  - Push to main branches → TestPyPI
  - Release published → PyPI
- **Jobs**:
  - `test`: Cross-platform testing
  - `build`: Package building
  - `publish`: PyPI upload
  - `publish-test`: TestPyPI upload

## 🔧 Manual Publishing (Alternative)

If you prefer manual control:

### 1. Build the Package

```bash
# Install build tools
pip install build twine

# Build package
python -m build

# Check package
twine check dist/*
```

### 2. Upload to PyPI

```bash
# Upload to PyPI
twine upload dist/*

# Or upload to TestPyPI first
twine upload --repository testpypi dist/*
```

## 🔐 API Token Method (Legacy)

If you can't use trusted publishing:

### 1. Create PyPI API Tokens
- **PyPI**: https://pypi.org/manage/account/token/
- **TestPyPI**: https://test.pypi.org/manage/account/token/

### 2. Add to GitHub Secrets
Go to your repository Settings → Secrets and variables → Actions:
- `PYPI_API_TOKEN`: Your PyPI token
- `TEST_PYPI_API_TOKEN`: Your TestPyPI token

### 3. Modify Workflow
Update `.github/workflows/publish.yml`:

```yaml
- name: Publish to PyPI
  run: |
    pip install twine
    twine upload -u __token__ -p ${{ secrets.PYPI_API_TOKEN }} dist/*
```

## 📦 Release Process

### Creating a Release

1. **Update version** in `setup.py`:
   ```python
   version='1.0.0',
   ```

2. **Commit changes**:
   ```bash
   git add setup.py
   git commit -m "Release version 1.0.0"
   git push origin main
   ```

3. **Create GitHub release**:
   - Go to Releases → Create new release
   - Tag: `v1.0.0`
   - Title: `Release v1.0.0`
   - Description: Release notes
   - Publish release

4. **Monitor Actions**:
   - The publish workflow will automatically run
   - Check the Actions tab for progress
   - Package will be published to PyPI

## 🧪 Testing

### Local Testing
```bash
# Test package build
python -m build
twine check dist/*

# Test installation
pip install dist/filehasher-*.tar.gz
```

### TestPyPI Testing
```bash
# Install from TestPyPI
pip install -i https://test.pypi.org/simple/ filehasher

# Test functionality
filehasher --help
filehasher --benchmark
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Trusted Publishing Fails
- Check environment names match exactly
- Verify repository owner/name are correct
- Ensure workflow file name matches

#### 2. Package Build Fails
```bash
# Check for missing dependencies
pip install -e .
python -c "import filehasher"

# Validate setup.py
python setup.py check
```

#### 3. Upload Fails
```bash
# Check package validity
twine check dist/*

# Verify API token permissions
# Try uploading manually first
```

#### 4. Test Failures
```bash
# Run tests locally
pip install -e .
filehasher --help
filehasher --generate --quiet
```

## 📚 Resources

- [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Python Packaging Guide](https://packaging.python.org/)
- [Twine Documentation](https://twine.readthedocs.io/)

## 🔄 Workflow Files

- `.github/workflows/ci.yml` - Continuous integration
- `.github/workflows/publish.yml` - Publishing pipeline
- `.github/PUBLISHING.md` - This documentation

---

**Happy publishing!** 🎉📦
