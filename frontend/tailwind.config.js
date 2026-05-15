/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ['./src/**/*.{js,ts,jsx,tsx,mdx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        bg: {
          primary: '#0f0f0f',
          secondary: '#1a1a1a',
          tertiary: '#242424',
          card: '#1e1e1e',
        },
        accent: {
          red: '#ff0000',
          redHover: '#cc0000',
          blue: '#3ea6ff',
        },
        text: {
          primary: '#f1f1f1',
          secondary: '#aaaaaa',
          muted: '#717171',
        },
        border: '#3a3a3a',
      },
      fontFamily: {
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'sans-serif'],
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'fade-in': 'fadeIn 0.3s ease-in-out',
      },
      keyframes: {
        fadeIn: { from: { opacity: 0 }, to: { opacity: 1 } },
      },
      aspectRatio: {
        video: '16 / 9',
      },
    },
  },
  plugins: [require('@tailwindcss/typography')],
};
